"""Pydantic request/response schemas (Day 3).

Validation is split in two layers, on purpose:

- STRUCTURAL validation lives here: field presence, types, non-empty
  sequence, no unknown fields. Pydantic rejects these before any
  application code runs, and FastAPI turns them into 422s for free.
- MODEL-DEPENDENT validation (token ids within the vocab) stays in
  ModelRunner._validate(), because the runner is the component that
  actually knows the vocab size. Duplicating the vocab bound in the
  schema would create two sources of truth that could drift.
"""

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    """One prediction request: a single variable-length token-id sequence."""

    tokens: list[int] = Field(
        ...,
        min_length=1,
        description="Token ids for one sequence; at least one required.",
    )

    # Reject unknown fields loudly instead of silently ignoring them --
    # a typo'd field name should be a 422, not a mystery.
    model_config = {"extra": "forbid"}


class PredictResponse(BaseModel):
    """One prediction result: the model head's n_outputs floats."""

    outputs: list[float] = Field(
        ...,
        description="Model outputs for the request, length n_outputs.",
    )
