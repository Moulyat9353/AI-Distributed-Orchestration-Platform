from transformers import pipeline

summarizer_pipeline = pipeline(
    "summarization",
    model="facebook/bart-large-cnn",
    max_length=130,
    min_length=30
)

sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english"
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

def analyze_sentiment(text: str) -> str:
    result = sentiment_pipeline(text[:512])[0]
    return f"{result['label']} (confidence: {result['score']:.2f})"

def extract_keywords(text: str) -> str:
    words = text.lower().split()
    stopwords = {"the","a","an","is","are","was","were","in","on",
                 "at","to","for","of","and","or","but","with","this",
                 "that","it","as","be","by","from","have","has"}
    freq = {}
    for word in words:
        word = word.strip(".,!?;:")
        if word not in stopwords and len(word) > 3:
            freq[word] = freq.get(word, 0) + 1
    top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]
    return ", ".join([word for word, _ in top])