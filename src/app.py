from trivia_question import TriviaQuestion


n_questions = 20
category = ""
difficulty = "medium"
type = "boolean"

def main():
    tq = TriviaQuestion(question=n_questions, category=category, difficulty=difficulty, type=type)
    print(tq.url)


if __name__ == "__main__":
    main()
