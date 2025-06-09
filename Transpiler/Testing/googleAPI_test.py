import google.generativeai as genai

# Read API key from file
with open("Transpiler/Testing/api_key.txt", "r") as file:
    api_key = file.read().strip()
# Configure the API key
genai.configure(api_key=api_key)
# Create the model
model = genai.GenerativeModel("gemini-2.0-flash-exp")
# Generate content
response = model.generate_content("Explain how AI works in a few words")
print(response.text)