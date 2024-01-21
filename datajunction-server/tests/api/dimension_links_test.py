"""Dimension linking related tests."""
from typing import Optional

import pytest
from requests import Response
from starlette.testclient import TestClient

from tests.conftest import post_and_raise_if_error
from tests.examples import COMPLEX_DIMENSION_LINK, EXAMPLES, SERVICE_SETUP
from tests.sql.utils import compare_query_strings


@pytest.fixture
def dimensions_link_client(client: TestClient) -> TestClient:
    """
    Add dimension link examples to the roads test client.
    """
    for endpoint, json in SERVICE_SETUP + COMPLEX_DIMENSION_LINK:
        post_and_raise_if_error(  # type: ignore
            client=client,
            endpoint=endpoint,
            json=json,  # type: ignore
        )
    return client


def test_link_dimension_with_errors(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Test linking dimensions with errors
    """
    response = dimensions_link_client.post(
        "/nodes/default.elapsed_secs/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.elapsed_secs.x = default.users.y"),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "Cannot link dimension to a node of type metric. Must be a source, "
        "dimension, or transform node."
    )
    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.users.user_id = default.users.user_id"),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "The join SQL provided does not reference both the origin node default.events "
        "and the dimension node default.users that it's being joined to."
    )

    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.events.order_year = default.users.year"),
            "join_cardinality": "many_to_one",
        },
    )
    assert (
        response.json()["message"]
        == "Join query default.events.order_year = default.users.year is not valid"
    )


@pytest.fixture
def link_events_with_users(dimensions_link_client: TestClient):
    """
    Link events with the users dimension
    """

    def _link_events_with_users(role: Optional[str] = None) -> Response:
        params = {
            "dimension_node": "default.users",
            "join_type": "left",
            "join_on": (
                "default.events.user_id = default.users.user_id "
                "AND default.events.event_start_date = default.users.snapshot_date"
            ),
            "join_cardinality": "one_to_one",
        }
        if role:
            params["role"] = role
        response = dimensions_link_client.post(
            "/nodes/default.events/link",
            json=params,
        )
        return response

    return _link_events_with_users


def test_link_complex_dimension_without_role(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name,
    link_events_with_users,
):
    """
    Test linking complex dimension without role
    """
    response = link_events_with_users(role=None)
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    response = dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": None,
        },
    ]

    # Update dimension link
    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_type": "left",
            "join_on": (
                "default.events.user_id = default.users.user_id "
                "AND default.events.event_end_date = default.users.snapshot_date"
            ),
            "join_cardinality": "one_to_many",
        },
    )
    assert response.json() == {
        "message": "The dimension link between default.events and "
        "default.users has been successfully updated.",
    }

    response = dimensions_link_client.get("/history?node=default.events")
    assert [
        (entry["activity_type"], entry["details"])
        for entry in response.json()
        if entry["entity_type"] == "link"
    ] == [
        (
            "create",
            {
                "dimension": "default.users",
                "join_cardinality": "one_to_one",
                "join_sql": "default.events.user_id = default.users.user_id AND "
                "default.events.event_start_date = default.users.snapshot_date",
                "role": None,
            },
        ),
        (
            "update",
            {
                "dimension": "default.users",
                "join_cardinality": "one_to_many",
                "join_sql": "default.events.user_id = default.users.user_id AND "
                "default.events.event_end_date = default.users.snapshot_date",
                "role": None,
            },
        ),
    ]

    # Switch back to original join definition
    link_events_with_users(role=None)

    response = dimensions_link_client.get(
        "/sql/default.events?dimensions=default.users.user_id"
        "&dimensions=default.users.snapshot_date"
        "&dimensions=default.users.registration_country",
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT  default_DOT_users.user_id default_DOT_events_DOT_user_id,
	default_DOT_events.event_start_date default_DOT_events_DOT_event_start_date,
	default_DOT_events.event_end_date default_DOT_events_DOT_event_end_date,
	default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
	default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
	default_DOT_users.registration_country default_DOT_users_DOT_registration_country 
 FROM (SELECT  default_DOT_events_table.user_id,
	default_DOT_events_table.event_start_date,
	default_DOT_events_table.event_end_date,
	default_DOT_events_table.elapsed_secs 
 FROM examples.events AS default_DOT_events_table)
 AS default_DOT_events LEFT  JOIN (SELECT  default_DOT_users.user_id,
	default_DOT_users.snapshot_date,
	default_DOT_users.registration_country,
	default_DOT_users.residence_country,
	default_DOT_users.account_type 
 FROM examples.users AS default_DOT_users
) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date""",
    )

    response = dimensions_link_client.get("/nodes/default.events/dimensions")
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ("default.users.account_type[]", ["default.events."]),
        ("default.users.registration_country[]", ["default.events."]),
        ("default.users.residence_country[]", ["default.events."]),
        ("default.users.snapshot_date[]", ["default.events."]),
        ("default.users.user_id[]", ["default.events."]),
    ]


def test_link_dimension_with_role(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
    link_events_with_users,
):
    response = link_events_with_users(role="user_direct")
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    response = dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": "user_direct",
        },
    ]

    # Add a dimension link with different role
    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_type": "left",
            "join_on": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date BETWEEN default.users.snapshot_date "
            "AND CAST(DATE_ADD(CAST(default.users.snapshot_date AS DATE), 10) AS INT)",
            "join_cardinality": "one_to_many",
            "role": "user_windowed",
        },
    )
    assert response.json() == {
        "message": "Dimension node default.users has been successfully linked to node "
        "default.events.",
    }

    # Add a dimension link on users for registration country
    response = dimensions_link_client.post(
        "/nodes/default.users/link",
        json={
            "dimension_node": "default.countries",
            "join_type": "inner",
            "join_on": "default.users.registration_country = default.countries.country_code ",
            "join_cardinality": "one_to_one",
            "role": "registration_country",
        },
    )
    assert response.json() == {
        "message": "Dimension node default.countries has been successfully linked to node "
        "default.users.",
    }

    # # Add a dimension link on users for registration country
    # response = dimensions_link_client.get(
    #     "/nodes/default.users",
    # )
    # assert response.json()["dimension_links"] == []

    response = dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id AND "
            "default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": "user_direct",
        },
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_many",
            "join_sql": "default.events.user_id = default.users.user_id AND "
            "default.events.event_start_date BETWEEN "
            "default.users.snapshot_date AND "
            "CAST(DATE_ADD(CAST(default.users.snapshot_date AS DATE), 10) AS "
            "INT)",
            "join_type": "left",
            "role": "user_windowed",
        },
    ]

    # Verify that the dimensions on the downstream metric have roles specified
    response = dimensions_link_client.get("/nodes/default.elapsed_secs/dimensions")
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ('default.countries.country_code[user_direct->registration_country]',
         ['default.events.user_direct', 'default.users.registration_country']),
        ('default.countries.country_code[user_windowed->registration_country]',
         ['default.events.user_windowed', 'default.users.registration_country']),
        ('default.countries.name[user_direct->registration_country]',
         ['default.events.user_direct', 'default.users.registration_country']),
        ('default.countries.name[user_windowed->registration_country]',
         ['default.events.user_windowed', 'default.users.registration_country']),
        ('default.countries.population[user_direct->registration_country]',
         ['default.events.user_direct', 'default.users.registration_country']),
        ('default.countries.population[user_windowed->registration_country]',
         ['default.events.user_windowed', 'default.users.registration_country']),
        ("default.users.account_type[user_direct]", ["default.events.user_direct"]),
        ("default.users.account_type[user_windowed]", ["default.events.user_windowed"]),
        (
            "default.users.registration_country[user_direct]",
            ["default.events.user_direct"],
        ),
        (
            "default.users.registration_country[user_windowed]",
            ["default.events.user_windowed"],
        ),
        (
            "default.users.residence_country[user_direct]",
            ["default.events.user_direct"],
        ),
        (
            "default.users.residence_country[user_windowed]",
            ["default.events.user_windowed"],
        ),
        ("default.users.snapshot_date[user_direct]", ["default.events.user_direct"]),
        (
            "default.users.snapshot_date[user_windowed]",
            ["default.events.user_windowed"],
        ),
        ("default.users.user_id[user_direct]", ["default.events.user_direct"]),
        ("default.users.user_id[user_windowed]", ["default.events.user_windowed"]),
    ]

    # Get SQL for the downstream metric grouped by the user dimension of role "user_windowed"
    response = dimensions_link_client.get(
        "/sql/default.elapsed_secs?dimensions=default.users.user_id[user_windowed]"
        "&dimensions=default.users.snapshot_date[user_windowed]"
        "&dimensions=default.users.registration_country[user_windowed]",
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT  SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs,
	default_DOT_users.user_id default_DOT_users_DOT_user_id,
	default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
	default_DOT_users.registration_country default_DOT_users_DOT_registration_country 
 FROM (SELECT  default_DOT_events_table.user_id,
	default_DOT_events_table.event_start_date,
	default_DOT_events_table.event_end_date,
	default_DOT_events_table.elapsed_secs 
 FROM examples.events AS default_DOT_events_table)
 AS default_DOT_events LEFT  JOIN (SELECT  default_DOT_users.user_id,
	default_DOT_users.snapshot_date,
	default_DOT_users.registration_country,
	default_DOT_users.residence_country,
	default_DOT_users.account_type 
 FROM examples.users AS default_DOT_users

) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id AND default_DOT_events.event_start_date BETWEEN default_DOT_users.snapshot_date AND CAST(DATE_ADD(CAST(default_DOT_users.snapshot_date AS DATE), 10) AS INT) 
 GROUP BY  default_DOT_users.user_id, default_DOT_users.snapshot_date, default_DOT_users.registration_country""",
    )

    # Get SQL for the downstream metric grouped by the user dimension of role "user"
    response = dimensions_link_client.get(
        "/sql/default.elapsed_secs?dimensions=default.users.user_id[user_direct]"
        "&dimensions=default.users.snapshot_date[user_direct]"
        "&dimensions=default.users.registration_country[user_direct]",
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT  SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs,
	default_DOT_users.user_id default_DOT_users_DOT_user_id,
	default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
	default_DOT_users.registration_country default_DOT_users_DOT_registration_country 
 FROM (SELECT  default_DOT_events_table.user_id,
	default_DOT_events_table.event_start_date,
	default_DOT_events_table.event_end_date,
	default_DOT_events_table.elapsed_secs 
 FROM examples.events AS default_DOT_events_table)
 AS default_DOT_events LEFT  JOIN (SELECT  default_DOT_users.user_id,
	default_DOT_users.snapshot_date,
	default_DOT_users.registration_country,
	default_DOT_users.residence_country,
	default_DOT_users.account_type 
 FROM examples.users AS default_DOT_users

) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date 
 GROUP BY  default_DOT_users.user_id, default_DOT_users.snapshot_date, default_DOT_users.registration_country""",
    )
