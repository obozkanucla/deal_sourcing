import openai
import json


def classify_listing(cleaned_text: str) -> dict:
    response = openai.ChatCompletion.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": "You are a classification engine for business-for-sale listings. Follow the JSON schema exactly."},
            {"role": "user", "content": cleaned_text}
        ],
        temperature=0
    )

    return json.loads(response.choices[0].message["content"])