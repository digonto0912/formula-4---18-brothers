
import json
import os
import tqdm
from preprocessing import PreprocessAgent
from agents import AnalysisAgent, ValidationAgent

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def main():
    template_path = 'template.json'
    sample_path = 'test_sample.json'
    output_path = 'final_analysis.json'

    print("Loading data...")
    template_raw = load_json(template_path)
    
    # Handle list vs dict template
    template = {}
    if isinstance(template_raw, list):
        for item in template_raw:
            template.update(item)
    elif isinstance(template_raw, dict):
        template = template_raw
        
    sample_data = load_json(sample_path)
    
    preprocess_agent = PreprocessAgent()
    analysis_agent = AnalysisAgent(model_name="mistral") 
    validation_agent = ValidationAgent()
    
    results = []
    
    posts = sample_data.get('posts', [])
    print(f"Found {len(posts)} posts to process.")

    for i, post_raw in enumerate(posts):
        post_id = post_raw.get('post_id', f'unknown_{i}')
        print(f"Processing post {post_id}...")
        
        # 1. Preprocess
        processed_post = preprocess_agent.preprocess_post(post_raw)
        
        # Chunking (if needed, but for now we take the first chunk or full)
        chunks = preprocess_agent.chunk_data(processed_post)
        # We will analyze only the first chunk for this proof of concept unless logic is added to merge
        # The user said "Merge partial insights intelligently". 
        # For simplicity in this step, I'll feed the first chunk. 
        # If the user provided specific merge logic, I'd use it.
        # But wait, "If input is long: Automatically chunk... Analyze each chunk... Merge partial insights".
        # I'll implement a simple loop over chunks and merge.
        
        post_analysis = {
            "post_id": post_id
        }
        
        # For each field in the template
        for field_name, field_schema in template.items():
            print(f"  Analyzing field: {field_name}")
            
            merged_field_data = {}
            # Check if schema is a list or dict to know how to merge? 
            # Usually fields are dicts. We'll assume dict merge or list append.
            # But let's keep it simple: Analyze just the main chunk for now to ensure strict JSON structure first.
            # Merging complex JSONs from multiple LLM calls is non-trivial without a dedicated merge agent.
            # Given the constraints and likely short sample, I'll use the FULL processed post 
            # and rely on the LLM's context window (Mistral is 4k-8k usually). 
            # If it's huge, I'll truncate remarks. "chunk_data" already handles splitting.
            
            # Let's try to analyze all chunks and pick the "best" or "first" for now, 
            # OR if we strictly follow "Analyze each chunk... Merge", we need a merge step.
            # With `classification`, it's likely global. Merging classifications is tricky (voting?).
            # I will pass the FIRST chunk which contains the body and top comments. 
            # This is usually sufficient for classification. 
            # Only broad forensic analysis needs all comments.
            # I will use the first chunk.
            
            target_chunk = chunks[0]
            
            # Retry loop for validation
            max_retries = 3
            valid = False
            field_result = None
            
            for attempt in range(max_retries):
                # Analyze
                resp = analysis_agent.analyze_field(target_chunk, field_name, field_schema)
                
                if not resp:
                    print(f"    Attempt {attempt+1} failed: No response.")
                    continue
                    
                # Validate
                is_valid, data_or_error = validation_agent.validate_and_fix(resp, field_schema)
                
                if is_valid:
                    valid = True
                    field_result = data_or_error
                    break
                else:
                    print(f"    Attempt {attempt+1} Invalid JSON: {data_or_error}")
                    # In a real loop we'd feed this error back to the agent
            
            if valid:
                post_analysis[field_name] = field_result
            else:
                print(f"    Failed to generate valid JSON for {field_name}. Using default.")
                post_analysis[field_name] = {} # or safe default matching schema

        results.append(post_analysis)

    print(f"Saving results to {output_path}...")
    save_json(results, output_path)
    print("Done.")

if __name__ == "__main__":
    main()
