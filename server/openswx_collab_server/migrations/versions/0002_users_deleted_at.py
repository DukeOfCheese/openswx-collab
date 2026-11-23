"""Accounts can be deleted: users.deleted_at

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-23 20:36:14.782523
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.add_column(sa.Column('deleted_at', sa.Float(), nullable=True))



def downgrade() -> None:
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.drop_column('deleted_at')

