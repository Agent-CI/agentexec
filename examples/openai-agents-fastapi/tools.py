from agents import function_tool


@function_tool
def search_company_info(company_name: str, query_type: str) -> str:
    """Search for company information.

    Args:
        company_name: The name of the company to search for
        query_type: Type of information to search for (financial, news, products, team)

    Returns:
        Relevant information about the company
    """
    # In a real implementation, this would call actual APIs
    # For demo purposes, return simulated data
    if query_type == "financial":
        return f"{company_name} reported $100M revenue last quarter with 20% YoY growth."
    elif query_type == "news":
        return f"{company_name} recently announced a new product launch and expansion plans."
    elif query_type == "products":
        return f"{company_name} offers SaaS solutions for enterprise data management."
    elif query_type == "team":
        return f"{company_name} has 500+ employees across 5 offices globally."
    return f"General information about {company_name}"


@function_tool
def analyze_financial_data(company_name: str, metrics: list[str]) -> dict:
    """Analyze financial metrics for a company.

    Args:
        company_name: The name of the company
        metrics: List of metrics to analyze (revenue, profit, growth_rate)

    Returns:
        Dictionary with analyzed metrics
    """
    # Simulated financial analysis
    return {
        "company": company_name,
        "revenue": "$100M",
        "profit_margin": "25%",
        "growth_rate": "20% YoY",
        "outlook": "positive",
    }
