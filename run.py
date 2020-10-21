from HelloFresh import HelloFresh

if __name__ == "__main__":
    hellofresh = HelloFresh("https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json")
    hellofresh.config(easy=30, hard=60)
    hellofresh.get("beef")
    hellofresh.save("output/report.csv")
    print(hellofresh)