import update_markets
import update_goldsky
import process_live

if __name__ == "__main__":
    print("Updating markets")
    update_markets()
    print("Updating goldsky")
    update_goldsky()
    print("Processing live")
    process_live()