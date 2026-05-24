from transformers import pipeline

summarizer_pipeline = pipeline(
    "summarization",
    model="facebook/bart-large-cnn",
    max_length=130,
    min_length=30
)

def summarize(text: str) -> str:
    chunks = [text[i:i+1000] for i in range(0, len(text), 1000)]
    summaries = []
    for chunk in chunks:
        if len(chunk.strip()) == 0:
            continue
        result = summarizer_pipeline(chunk, truncation=True)
        summaries.append(result[0]["summary_text"])
    return " ".join(summaries)