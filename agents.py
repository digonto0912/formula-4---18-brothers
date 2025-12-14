
import json
import subprocess
import shlex
import os

class AnalysisAgent:
    def __init__(self, model_name="mistral"):
        self.model_name = model_name

    def generate(self, prompt):
        """
        Calls Ollama via subprocess.
        """
        try:
            # Set environment to force UTF-8
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            
            # Using stdin for the prompt to avoid shell length limits
            # Pass input as bytes to avoid encoding errors in text mode on Windows
            process = subprocess.Popen(
                ["ollama", "run", self.model_name],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            
            # Encode prompt to utf-8 bytes
            input_bytes = prompt.encode('utf-8')
            stdout_bytes, stderr_bytes = process.communicate(input=input_bytes)
            
            if process.returncode != 0:
                print(f"Error calling Ollama: {stderr_bytes.decode('utf-8', errors='replace')}")
                return None
            
            # Decode output
            return stdout_bytes.decode('utf-8', errors='replace').strip()
        except Exception as e:
            print(f"Exception calling Ollama: {e}")
            return None

    def analyze_field(self, data_chunk, field_name, field_schema):
        """
        Analyzes a specific field based on the schema.
        """
        system_prompt = f"""You are a forensic data analyst.
TASK: Analyze the provided Reddit post and comments to populate the '{field_name}' field.
The output MUST be a valid JSON object strictly matching the schema for '{field_name}'.

CRITICAL DEFINITIONS (NON-NEGOTIABLE):
1. topic_category:
   - MUST describe the specific subject of THIS post (e.g., "Speculation about robotic assistance in pregnancy").
   - NO generic labels (e.g., "Science & Technology", "Discussion").
   - Should feel like a precise headline.
2. user_intent:
   - MUST capture the psychological and communicative intent (e.g., "Testing social acceptance of a taboo idea").
   - NO generic intents (e.g., "Asking a question").
   - MUST include how commenters are engaging (e.g., "normalization", "outrage").

QUALITY RULES:
- Be forensic, psychological, and context-aware.
- Comments are NOT optional — they must influence interpretation for 'user_intent'.
- Assume the reader is intelligent — no obvious facts.

SCHEMA:
{json.dumps(field_schema, indent=2)}

DATA:
{json.dumps(data_chunk, indent=2)}

INSTRUCTIONS:
1. Output ONLY the JSON object.
2. NO markdown formatting.
3. NO explanations.
"""
        return self.generate(system_prompt)

class ValidationAgent:
    def __init__(self):
        pass

    def extract_json(self, text):
        """
        Attempts to find the largest JSON object in the text.
        """
        text = text.strip()
        # Remove markdown code blocks
        if text.startswith("```"):
            lines = text.splitlines()
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
            
        # Try finding the first '{' and last '}'
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            possible_json = text[start:end+1]
            return possible_json
        return text

    def validate_and_fix(self, json_str, expected_schema):
        """
        Tries to parse JSON.
        """
        try:
            cleaned = self.extract_json(json_str)
            data = json.loads(cleaned)
            return True, data
        except json.JSONDecodeError as e:
            # Simple retry/fix logic could go here (e.g. escaping quotes)
            return False, f"JSON Decode Error: {e} | Text: {json_str[:50]}..."
        except Exception as e:
             return False, str(e)

if __name__ == "__main__":
    # Test
    agent = AnalysisAgent()
    res = agent.generate("Say 'hello' in JSON format: {\"msg\": \"hello\"}")
    print(res)
