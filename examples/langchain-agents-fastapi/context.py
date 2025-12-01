"""Context classes for LangChain Agents FastAPI example.

These Pydantic models provide type-safe context for background tasks.
"""

from pydantic import BaseModel, Field


class ResearchCompanyContext(BaseModel):
    """Context for company research tasks."""

    company_name: str = Field(..., min_length=1, description="Name of the company to research")
    input_prompt: str | None = Field(None, description="Custom research prompt (optional)")

    class Config:
        json_schema_extra = {
            "example": {
                "company_name": "Anthropic",
                "input_prompt": "Focus on their AI safety research and product offerings",
            }
        }
