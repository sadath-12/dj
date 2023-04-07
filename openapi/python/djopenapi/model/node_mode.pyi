# coding: utf-8

"""
    DJ server

    A DataJunction metrics layer  # noqa: E501

    The version of the OpenAPI document: 0.0.post1.dev1+g1e86dc4
    Generated by: https://openapi-generator.tech
"""

from datetime import date, datetime  # noqa: F401
import decimal  # noqa: F401
import functools  # noqa: F401
import io  # noqa: F401
import re  # noqa: F401
import typing  # noqa: F401
import typing_extensions  # noqa: F401
import uuid  # noqa: F401

import frozendict  # noqa: F401

from djopenapi import schemas  # noqa: F401


class NodeMode(
    schemas.EnumBase,
    schemas.StrSchema
):
    """NOTE: This class is auto generated by OpenAPI Generator.
    Ref: https://openapi-generator.tech

    Do not edit the class manually.

    Node mode.

A node can be in one of the following modes:

1. PUBLISHED - Must be valid and not cause any child nodes to be invalid
2. DRAFT - Can be invalid, have invalid parents, and include dangling references
    """
    
    @schemas.classproperty
    def PUBLISHED(cls):
        return cls("published")
    
    @schemas.classproperty
    def DRAFT(cls):
        return cls("draft")
