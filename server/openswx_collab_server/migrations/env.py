"""Alembic environment. Run through openswx_collab_server.migrate (the server does it on start), not the
alembic command line: the connection comes from there, already open."""
from __future__ import annotations

from alembic import context

from openswx_collab_server.db import Base

connection = context.config.attributes["connection"]


def include_object(obj, name, type_, reflected, compare_to):
    # tables that are not the open server's (an extension's, say) are none of these migrations' business
    if type_ == "table" and reflected and compare_to is None:
        return False
    return True


context.configure(connection=connection, target_metadata=Base.metadata, render_as_batch=True,
                  compare_type=True, include_object=include_object)
with context.begin_transaction():
    context.run_migrations()
