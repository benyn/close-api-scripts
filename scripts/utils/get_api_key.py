import shlex
import subprocess


def get_api_key(keychain_item_name, account_name):
    # Construct the command to retrieve the password
    command = (
        f"security find-generic-password -a {account_name} -s {keychain_item_name} -w"
    )

    # Use shlex to safely format the command
    safe_command = shlex.split(command)

    try:
        # Run the command
        result = subprocess.run(
            safe_command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # The API key is in the standard output
        return result.stdout.strip()

    except subprocess.CalledProcessError as e:
        # Handle errors if the command fails
        print(f"Error: {e.stderr}")
        return None
