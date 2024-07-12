"""
Node namespace related APIs.
"""
import logging
from http import HTTPStatus
from typing import Dict, List, Optional

from fastapi import Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_node_namespace
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.errors import DJAlreadyExistsException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
    validate_access_requests,
)
from datajunction_server.internal.namespaces import (
    create_namespace,
    get_nodes_in_namespace,
    get_nodes_in_namespace_detailed,
    get_project_config,
    hard_delete_namespace,
    mark_namespace_deactivated,
    mark_namespace_restored,
    validate_namespace,
)
from datajunction_server.internal.nodes import activate_node, deactivate_node
from datajunction_server.models import access
from datajunction_server.models.node import NamespaceOutput, NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import (
    get_and_update_current_user,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["namespaces"])


@router.post("/namespaces/{namespace}/", status_code=HTTPStatus.CREATED)
async def create_node_namespace(
    namespace: str,
    include_parents: Optional[bool] = False,
    session: AsyncSession = Depends(get_session),
    current_user: Optional[User] = Depends(get_and_update_current_user),
) -> JSONResponse:
    """
    Create a node namespace
    """
    if node_namespace := await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=False,
    ):  # pragma: no cover
        if node_namespace.deactivated_at:
            node_namespace.deactivated_at = None
            session.add(node_namespace)
            await session.commit()
            return JSONResponse(
                status_code=HTTPStatus.CREATED,
                content={
                    "message": (
                        "The following node namespace has been successfully reactivated: "
                        + namespace
                    ),
                },
            )
        return JSONResponse(
            status_code=409,
            content={
                "message": f"Node namespace `{namespace}` already exists",
            },
        )
    validate_namespace(namespace)
    created_namespaces = await create_namespace(
        session,
        namespace,
        include_parents,  # type: ignore
        current_user=current_user,
    )
    return JSONResponse(
        status_code=HTTPStatus.CREATED,
        content={
            "message": (
                "The following node namespaces have been successfully created: "
                + ", ".join(created_namespaces)
            ),
        },
    )


@router.get(
    "/namespaces/",
    response_model=List[NamespaceOutput],
    status_code=200,
)
async def list_namespaces(
    session: AsyncSession = Depends(get_session),
    current_user: Optional[User] = Depends(get_and_update_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> List[NamespaceOutput]:
    """
    List namespaces with the number of nodes contained in them
    """
    results = await NodeNamespace.get_all_with_node_count(session)
    resource_requests = [
        access.ResourceRequest(
            verb=access.ResourceRequestVerb.BROWSE,
            access_object=access.Resource.from_namespace(record.namespace),
        )
        for record in results
    ]
    approvals = validate_access_requests(
        validate_access,
        current_user,
        resource_requests=resource_requests,
    )
    approved_namespaces: List[str] = [
        request.access_object.name for request in approvals
    ]
    return [
        NamespaceOutput(namespace=record.namespace, num_nodes=record.num_nodes)
        for record in results
        if record.namespace in approved_namespaces
    ]


@router.get(
    "/namespaces/{namespace}/",
    response_model=List[NodeMinimumDetail],
    status_code=HTTPStatus.OK,
)
async def list_nodes_in_namespace(
    namespace: str,
    type_: Optional[NodeType] = Query(
        default=None,
        description="Filter the list of nodes to this type",
    ),
    session: AsyncSession = Depends(get_session),
) -> List[NodeMinimumDetail]:
    """
    List node names in namespace, filterable to a given type if desired.
    """
    return await NodeNamespace.list_nodes(session, namespace, type_)


@router.delete("/namespaces/{namespace}/", status_code=HTTPStatus.OK)
async def deactivate_a_namespace(
    namespace: str,
    cascade: bool = Query(
        default=False,
        description="Cascade the deletion down to the nodes in the namespace",
    ),
    session: AsyncSession = Depends(get_session),
    current_user: Optional[User] = Depends(get_and_update_current_user),
) -> JSONResponse:
    """
    Deactivates a node namespace
    """
    node_namespace = await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=True,
    )

    if node_namespace.deactivated_at:  # type: ignore
        raise DJAlreadyExistsException(
            message=f"Namespace `{namespace}` is already deactivated.",
        )

    # If there are no active nodes in the namespace, we can safely deactivate this namespace
    node_list = await NodeNamespace.list_nodes(session, namespace)
    node_names = [node.name for node in node_list]
    if len(node_names) == 0:
        message = f"Namespace `{namespace}` has been deactivated."
        await mark_namespace_deactivated(session, node_namespace, message)  # type: ignore
        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={"message": message},
        )

    # If cascade=true is set, we'll deactivate all nodes in this namespace and then
    # subsequently deactivate this namespace
    if cascade:
        for node_name in node_names:
            await deactivate_node(
                session,
                node_name,
                f"Cascaded from deactivating namespace `{namespace}`",
                current_user=current_user,
            )
        message = (
            f"Namespace `{namespace}` has been deactivated. The following nodes"
            f" have also been deactivated: {','.join(node_names)}"
        )
        await mark_namespace_deactivated(
            session,
            node_namespace,  # type: ignore
            message,
            current_user=current_user,
        )

        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={
                "message": message,
            },
        )

    return JSONResponse(
        status_code=405,
        content={
            "message": f"Cannot deactivate node namespace `{namespace}` as there are "
            "still active nodes under that namespace.",
        },
    )


@router.post("/namespaces/{namespace}/restore/", status_code=HTTPStatus.CREATED)
async def restore_a_namespace(
    namespace: str,
    cascade: bool = Query(
        default=False,
        description="Cascade the restore down to the nodes in the namespace",
    ),
    session: AsyncSession = Depends(get_session),
    current_user: Optional[User] = Depends(get_and_update_current_user),
) -> JSONResponse:
    """
    Restores a node namespace
    """
    node_namespace = await get_node_namespace(
        session=session,
        namespace=namespace,
        raise_if_not_exists=True,
    )
    if not node_namespace.deactivated_at:
        raise DJAlreadyExistsException(
            message=f"Node namespace `{namespace}` already exists and is active.",
        )

    node_list = await get_nodes_in_namespace(
        session,
        namespace,
        include_deactivated=True,
    )
    node_names = [node.name for node in node_list]
    # If cascade=true is set, we'll restore all nodes in this namespace and then
    # subsequently restore this namespace
    if cascade:
        for node_name in node_names:
            await activate_node(
                name=node_name,
                session=session,
                message=f"Cascaded from restoring namespace `{namespace}`",
                current_user=current_user,
            )

        message = (
            f"Namespace `{namespace}` has been restored. The following nodes"
            f" have also been restored: {','.join(node_names)}"
        )
        await mark_namespace_restored(session, node_namespace, message)

        return JSONResponse(
            status_code=HTTPStatus.CREATED,
            content={
                "message": message,
            },
        )

    # Otherwise just restore this namespace
    message = f"Namespace `{namespace}` has been restored."
    await mark_namespace_restored(
        session,
        node_namespace,
        message,
        current_user=current_user,
    )
    return JSONResponse(
        status_code=HTTPStatus.CREATED,
        content={"message": message},
    )


@router.delete("/namespaces/{namespace}/hard/", name="Hard Delete a DJ Namespace")
async def hard_delete_node_namespace(
    namespace: str,
    *,
    cascade: bool = False,
    session: AsyncSession = Depends(get_session),
    current_user: Optional[User] = Depends(get_and_update_current_user),
) -> JSONResponse:
    """
    Hard delete a namespace, which will completely remove the namespace. Additionally,
    if any nodes are saved under this namespace, we'll hard delete the nodes if cascade
    is set to true. If cascade is set to false, we'll raise an error. This should be used
    with caution, as the impact may be large.
    """
    impacts = await hard_delete_namespace(
        session=session,
        namespace=namespace,
        cascade=cascade,
        current_user=current_user,
    )
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"The namespace `{namespace}` has been completely removed.",
            "impact": impacts,
        },
    )


@router.get(
    "/namespaces/{namespace}/export/",
    name="Export a namespace as a single project's metadata",
)
async def export_a_namespace(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> List[Dict]:
    """
    Generates a zip of YAML files for the contents of the given namespace
    as well as a project definition file.
    """
    return await get_project_config(
        session=session,
        nodes=await get_nodes_in_namespace_detailed(session, namespace),
        namespace_requested=namespace,
    )
