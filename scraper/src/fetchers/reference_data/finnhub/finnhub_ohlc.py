import finnhub
import json

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key="cvmac61r01qnndmd61t0cvmac61r01qnndmd61tg")

# Get forex symbols for OANDA
forex_symbols = finnhub_client.forex_symbols("OANDA")

# Convert to JSON string
forex_symbols_json = json.dumps(forex_symbols, indent=4)

# Print the JSON output
print(forex_symbols_json)
