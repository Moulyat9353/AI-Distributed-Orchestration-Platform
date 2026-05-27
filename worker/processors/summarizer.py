from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
from textblob import TextBlob
import nltk

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)

def summarize(text: str) -> str:
    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = LsaSummarizer()
    summary = summarizer(parser.document, 2)
    return " ".join([str(sentence) for sentence in summary])

def analyze_sentiment(text: str) -> str:
    analysis = TextBlob(text)
    score = analysis.sentiment.polarity
    if score > 0.1:
        label = "POSITIVE"
    elif score < -0.1:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"
    return f"{label} (confidence: {abs(score):.2f})"

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