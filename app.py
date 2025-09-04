import os,json
import pandas as pd
import feedparser
import urllib.parse
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore


load_dotenv()
db_key=os.getenv("DB_KEY")

firebase_key=json.loads(db_key)


cred = credentials.Certificate(firebase_key)
firebase_admin.initialize_app(cred)

db=firestore.client()



# ---------------- PRIORITY ASSIGNMENT ----------------
def assign_priority(article):
    """Decide priority score based on title + summary."""
    text = (article.get("title", "") + " " + article.get("summary", "")).lower()

    # Act of God (highest priority)
    act_of_god = ["landslide", "flood", "earthquake", "avalanche", "cyclone", "cyclonic",
                  "storm", "heavy rain", "rough sea", "heavy tides", "tsunami", "cloudburst"]
    if any(word in text for word in act_of_god):
        return 10

    # Human-caused
    human_caused = ["dacoit", "robbery", "attack", "crime", "kidnap", "murder", "terrorist", "violence"]
    if any(word in text for word in human_caused):
        return 8

    # Animal-related
    animal_related = ["tiger", "leopard", "elephant", "bear", "snake", "crocodile", "monkey", "wild boar"]
    if any(word in text for word in animal_related):
        return 6

    # Normal accidents
    accidents = ["road accident", "bus crash", "train accident", "fire", "mishap", "boat capsize"]
    if any(word in text for word in accidents):
        return 4

    # Default (low risk news)
    return 2


# ---------------- FETCH NEWS ----------------
def fetch_news(district, state):
    """Fetch tourism safety related news for a district."""
    query = f"{district}, {state} tourism safety"
    encoded_query = urllib.parse.quote(query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"

    feed = feedparser.parse(rss_url)
    articles = []
    for entry in feed.entries[:5]:  # top 5 news
        article = {
            "title": entry.title,
            "link": entry.link,
            "published": entry.published,
            "summary": getattr(entry, "summary", ""),
        }
        article["priority"] = assign_priority(article)
        articles.append(article)
    return articles

# ---------------- SAVE TO FIRESTORE ----------------
def save_to_firestore(state, district, news_items):
    """Save news under states/{state}/{district}/{news_doc}"""
    state_ref = db.collection("states").document(state)
    district_ref = state_ref.collection(district)

    for article in news_items:
        url = article["link"]

        # Check for duplicates (by link)
        existing = district_ref.where("link", "==", url).get()
        if not existing:
            district_ref.add({
                "title": article["title"],
                "link": url,
                "published": article["published"],
                "priority": article["priority"]
            })

# ---------------- MAIN PIPELINE ----------------
def main():
    # Load Excel file (replace with your file path)
    df = pd.read_excel("districts.xlsx")

    # Normalize column names
    df.columns = df.columns.str.strip()

    for _, row in df.iterrows():
        try:
            state = str(row["State"]).strip()
            district = str(row["District"]).strip()

            print(f"📡 Fetching news for {district}, {state}...")
            news_items = fetch_news(district, state)
            save_to_firestore(state, district, news_items)

            print(f"✅ Saved {len(news_items)} articles for {district}, {state}")

        except Exception as e:
            print(f"❌ Error for {row}: {e}")

if __name__ == "__main__":
    main()

