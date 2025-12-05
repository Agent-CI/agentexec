from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import create_engine

import agentexec as ax

engine = create_engine("sqlite:///agents.db", echo=False)
ax.Base.metadata.create_all(engine)

pool = ax.WorkerPool(engine=engine)


class CompanyContext(BaseModel):
    """Initial input to the pipeline."""

    company_name: str = Field(..., description="Name of the company to research")


class AnalysisContext(BaseModel):
    """Context for final analysis task."""

    company_name: str
    brand_summary: str
    market_summary: str
    product_summary: str


class BrandResearchResult(BaseModel):
    """Result from brand research."""

    company_name: str
    website_url: str | None = None
    founding_year: int | None = None
    mission_statement: str | None = None
    brand_summary: str


class MarketResearchResult(BaseModel):
    """Result from market research."""

    company_name: str
    market_size: str | None = None
    competitors: list[str] = Field(default_factory=list)
    market_position: str | None = None
    market_summary: str


class ProductResearchResult(BaseModel):
    """Result from product research."""

    company_name: str
    products: list[str] = Field(default_factory=list)
    pricing_model: str | None = None
    product_summary: str


class CompanyReport(BaseModel):
    """Aggregated company research."""

    company_name: str
    brand: BrandResearchResult
    market: MarketResearchResult
    products: ProductResearchResult


class FinalAnalysis(BaseModel):
    """Final analysis result."""

    company_name: str
    executive_summary: str
    strengths: list[str]
    weaknesses: list[str]
    opportunities: list[str]
    recommendation: str


@pool.task("brand_research")
async def brand_research(agent_id: UUID, context: CompanyContext) -> BrandResearchResult:
    """Research company brand and identity."""
    # In a real implementation, this would use an AI agent
    # For demo, return mock data
    return BrandResearchResult(
        company_name=context.company_name,
        website_url=f"https://{context.company_name.lower()}.com",
        founding_year=2020,
        mission_statement="Building the future of AI",
        brand_summary=f"{context.company_name} is a technology company focused on AI.",
    )


@pool.task("market_research")
async def market_research(agent_id: UUID, context: CompanyContext) -> MarketResearchResult:
    """Research company's market position."""
    return MarketResearchResult(
        company_name=context.company_name,
        market_size="$50B",
        competitors=["Competitor A", "Competitor B"],
        market_position="Leader in AI safety",
        market_summary=f"{context.company_name} operates in a growing market.",
    )


@pool.task("product_research")
async def product_research(agent_id: UUID, context: CompanyContext) -> ProductResearchResult:
    """Research company's products and services."""
    return ProductResearchResult(
        company_name=context.company_name,
        products=["Product A", "Product B", "API Services"],
        pricing_model="Usage-based",
        product_summary=f"{context.company_name} offers a range of AI products.",
    )


@pool.task("final_analysis")
async def final_analysis(agent_id: UUID, context: AnalysisContext) -> FinalAnalysis:
    """Generate final analysis from all research."""
    return FinalAnalysis(
        company_name=context.company_name,
        executive_summary=(
            f"Comprehensive analysis of {context.company_name} based on brand, "
            "market, and product research."
        ),
        strengths=["Strong brand", "Growing market", "Innovative products"],
        weaknesses=["Competition", "Market volatility"],
        opportunities=["Market expansion", "New product lines"],
        recommendation=f"Positive outlook for {context.company_name}",
    )


pipeline = ax.Pipeline(pool=pool)


class CompanyResearchPipeline(pipeline.Base):
    """Multi-step pipeline for comprehensive company research.

    Steps:
    1. Run brand, market, and product research in parallel
    2. Aggregate results into a unified report
    3. Generate final analysis
    """

    @pipeline.step(0)
    async def parallel_research(
        self,
        input_ctx: CompanyContext,
    ) -> tuple[BrandResearchResult, MarketResearchResult, ProductResearchResult]:
        """Step 0: Run three research tasks in parallel."""
        company = input_ctx.company_name

        # Enqueue all research tasks
        brand_task = await ax.enqueue(
            "brand_research",
            CompanyContext(company_name=company),
        )
        market_task = await ax.enqueue(
            "market_research",
            CompanyContext(company_name=company),
        )
        product_task = await ax.enqueue(
            "product_research",
            CompanyContext(company_name=company),
        )

        # Wait for all to complete
        return await ax.gather(brand_task, market_task, product_task)

    @pipeline.step(1)
    async def aggregate_results(
        self,
        brand: BrandResearchResult,
        market: MarketResearchResult,
        products: ProductResearchResult,
    ) -> CompanyReport:
        """Step 1: Aggregate research results into a report.

        This runs locally (no task queue) since it's just data transformation.
        """
        return CompanyReport(
            company_name=brand.company_name,
            brand=brand,
            market=market,
            products=products,
        )

    @pipeline.step(2)
    async def analyze(self, report: CompanyReport) -> FinalAnalysis:
        """Step 2: Run final analysis task."""
        task = await ax.enqueue(
            "final_analysis",
            AnalysisContext(
                company_name=report.company_name,
                brand_summary=report.brand.brand_summary,
                market_summary=report.market.market_summary,
                product_summary=report.products.product_summary,
            ),
        )
        return await ax.get_result(task)


async def run_company_research(company_name: str) -> FinalAnalysis:
    """Execute the full research pipeline for a company.

    Args:
        company_name: Name of the company to research

    Returns:
        Final analysis with recommendations
    """
    # run in this thread (blocking
    # result = await pipeline.run(...)

    # run in the task queue
    task = await pipeline.enqueue(
        context=CompanyContext(company_name=company_name),
    )
    result = await ax.get_result(task)
    return result


if __name__ == "__main__":
    import asyncio
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        # Run as worker: python pipeline.py worker
        print("Starting workers...")
        print(f"Queue: {ax.CONF.queue_name}")
        print("Press Ctrl+C to stop")
        pool.run()
    else:
        # Run pipeline: python pipeline.py
        # NOTE: Workers must be running in another terminal first!
        #   python pipeline.py worker

        async def main() -> None:
            print("Starting company research pipeline...")
            print("(Make sure workers are running: python pipeline.py worker)")
            print()
            result = await run_company_research("Anthropic")
            print(f"\n=== Final Analysis for {result.company_name} ===")
            print(f"Summary: {result.executive_summary}")
            print(f"Strengths: {', '.join(result.strengths)}")
            print(f"Recommendation: {result.recommendation}")

        asyncio.run(main())
