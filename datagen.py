"""Hands-on L9: data generator (plain Python, standard library only).

    python3 datagen.py <seed> [output directory]

Writes users.csv and posts.csv for a small social network. The seed makes the data yours
and reproducible: the grader regenerates it from the seed you write in your report.
Default output directory: shared-folder/input (next to this file).
"""
import csv, os, random, sys
from datetime import datetime, timedelta

if len(sys.argv) < 2:
    print(__doc__); sys.exit(2)
seed = int(sys.argv[1])
out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared-folder", "input")
os.makedirs(out, exist_ok=True)
rnd = random.Random(seed)

N_USERS, N_POSTS = 50, 5000
handles = ["techie", "critic", "daily_vibes", "designer", "rage_user", "meme_lord", "social_queen",
           "calm_mind", "pixel_pusher", "stream_bot", "night_coder", "coffee_first"]
age_groups = ["Teen", "Adult", "Senior"]
countries = ["US", "UK", "Canada", "India", "Germany", "Brazil"]

users = []
for i in range(1, N_USERS + 1):
    users.append({
        "user_id": f"U{i:03d}",
        "username": f"@{rnd.choice(handles)}{i}",
        "age_group": rnd.choice(age_groups),
        "country": rnd.choice(countries),
        "verified": "true" if rnd.random() < 0.3 else "false",
    })
with open(os.path.join(out, "users.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(users[0].keys())); w.writeheader(); w.writerows(users)

# hashtags appear with inconsistent capitalization, as they do in real feeds
tags = ["#tech", "#fail", "#design", "#UX", "#cleanUI", "#mood", "#bug", "#love", "#social", "#AI", "#update", "#darkmode"]
tag_weights = [rnd.choice([1, 2, 3, 5, 8]) for _ in tags]
def spell(tag):
    r = rnd.random()
    return tag.lower() if r < 0.6 else (tag.upper() if r < 0.75 else tag)
contents = ["Loving the new update!", "This app keeps crashing. So annoying.", "Just another day...",
            "Absolutely love the UX!", "Worst experience ever.", "Such a smooth interface!",
            "Great performance on mobile.", "Can't stop using it!", "Needs dark mode ASAP!",
            "I'm impressed with the speed.", "Meh.", "Who approved this redesign?"]
# each user has an activity level, so a few users dominate the feed
weights = [rnd.choice([1, 1, 1, 2, 3, 6]) for _ in users]
base = datetime(2026, 9, 1)
posts = []
for pid in range(1, N_POSTS + 1):
    u = rnd.choices(users, weights=weights, k=1)[0]
    ts = base + timedelta(seconds=rnd.randint(0, 30 * 24 * 3600 - 1))
    sentiment = round(max(-1.0, min(1.0, rnd.gauss(0.1, 0.5))), 2)
    # engagement is correlated with sentiment and with being verified, so the tasks have something to find
    boost = 1.6 if u["verified"] == "true" else 1.0
    likes = int(max(0, rnd.gauss(40 + 40 * sentiment, 25)) * boost)
    retweets = int(max(0, rnd.gauss(10 + 10 * sentiment, 8)) * boost)
    k = rnd.randint(1, 3)
    chosen = []
    while len(chosen) < k:                       # weighted, without repeats: some tags trend
        t = rnd.choices(tags, weights=tag_weights, k=1)[0]
        if t not in chosen: chosen.append(t)
    hashtags = ",".join(spell(t) for t in chosen)
    posts.append({
        "post_id": f"P{pid:05d}", "user_id": u["user_id"], "content": rnd.choice(contents),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "likes": likes, "retweets": retweets,
        "hashtags": hashtags, "sentiment_score": sentiment,
    })
posts.sort(key=lambda p: p["timestamp"])
with open(os.path.join(out, "posts.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(posts[0].keys())); w.writeheader(); w.writerows(posts)
print(f"wrote {len(users)} users and {len(posts)} posts to {out} (seed {seed})")
