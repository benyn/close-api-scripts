def prompt_user_for_choice(field_name: str, options: list[str]) -> str | None:
    if not options:
        return None

    if len(options) == 1:
        return options[0]

    prompt_message = "\n".join(
        [f"{index + 1}. {option}" for index, option in enumerate(options)]
    )
    valid_options = " or ".join(str(index + 1) for index in range(len(options)))
    while True:
        print(f"Choose the new value for '{field_name}':")
        print(prompt_message)
        choice = input(f"Enter {valid_options}: ")
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice) - 1]
        else:
            print("Invalid input.")
