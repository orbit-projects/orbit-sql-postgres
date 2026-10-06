# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Validated, secret-aware connection and pool settings for PostgreSQL."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictBool, model_validator


class PostgresConfig(BaseModel):
    """Provider-specific connection settings with TLS enabled by default."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dsn: SecretStr = Field(min_length=1, max_length=2_048)
    min_pool_size: int = Field(default=1, ge=1, le=128, strict=True)
    max_pool_size: int = Field(default=10, ge=1, le=128, strict=True)
    command_timeout: float = Field(default=30.0, gt=0, le=300, allow_inf_nan=False, strict=True)
    connect_timeout: float = Field(default=10.0, gt=0, le=300, allow_inf_nan=False, strict=True)
    close_timeout: float = Field(default=10.0, gt=0, le=300, allow_inf_nan=False, strict=True)
    ssl: StrictBool = True

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> PostgresConfig:
        """Require an ordered pool range before handing settings to asyncpg."""
        if self.min_pool_size > self.max_pool_size:
            raise ValueError("min_pool_size must not exceed max_pool_size.")
        return self


__all__ = ["PostgresConfig"]
