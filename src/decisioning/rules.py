def apply_hard_rules(llm_output: dict) -> str:
    ebitda = llm_output["extracted_fields"]["financials"]["ebitda"]["amount"]

    if isinstance(ebitda, (int, float)) and ebitda < 1_000_000:
        return "Exclude"

    return llm_output["classification"]["decision"]