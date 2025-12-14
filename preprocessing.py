
import json
import re
from datetime import datetime

class PreprocessAgent:
    def __init__(self):
        pass

    def clean_text(self, text):
        if not text:
            return ""
        # Remove null bytes or invisible control characters
        text = text.replace('\u0000', '')
        # Simple markdown stripper (can be enhanced)
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1', text) # Links
        return text.strip()

    def build_comment_tree(self, comments):
        """
        Reconstructs a nested comment tree from a flat list of comments.
        Assumes 'parent_id' and 'comment_id' keys exist.
        Reddit prefixes: t1_ (comment), t3_ (post).
        """
        comment_map = {c['comment_id']: c for c in comments}
        roots = []

        # Initialize children list for all comments
        for c in comments:
            c['replies'] = []
            c['body'] = self.clean_text(c.get('body', ''))

        for c in comments:
            parent_id = c.get('parent_id', '')
            # Check if parent is a comment (t1_)
            if parent_id.startswith('t1_'):
                parent_id_clean = parent_id[3:]
                if parent_id_clean in comment_map:
                    comment_map[parent_id_clean]['replies'].append(c)
                else:
                    # Parent missing, treat as root or orphan (add to roots for now)
                    roots.append(c)
            else:
                # Parent is likely the post (t3_) or empty, so it's a top-level comment
                roots.append(c)

        return roots

    def preprocess_post(self, post_data):
        """
        Processes a single post object.
        """
        processed = {
            "post_id": post_data.get("post_id"),
            "title": self.clean_text(post_data.get("title", "")),
            "body": self.clean_text(post_data.get("body", "")),
            "author": post_data.get("author"),
            "created_utc": post_data.get("created_utc"),
            "subreddit": post_data.get("subreddit"),
            "comments": self.build_comment_tree(post_data.get("comments", []))
        }
        return processed

    def chunk_data(self, processed_data, max_chars=12000):
        """
        Chunks the processed data if it's too large for the context window.
        Preserves the post body in the first chunk and distributes comments.
        Simple logic: Serialize and check size, if too big, split comments.
        """
        # This is a basic implementation. For improved handling, token counting would be better.
        # But char count is a fast proxy.
        
        full_json = json.dumps(processed_data)
        if len(full_json) <= max_chars:
            return [processed_data]

        chunks = []
        base_post = {k: v for k, v in processed_data.items() if k != 'comments'}
        comments = processed_data.get('comments', [])
        
        current_chunk_comments = []
        current_size = len(json.dumps(base_post))
        
        for comment in comments:
            comment_json = json.dumps(comment)
            if current_size + len(comment_json) > max_chars and current_chunk_comments:
                # Flush current chunk
                chunk = base_post.copy()
                chunk['comments'] = current_chunk_comments
                chunk['is_chunk'] = True
                chunk['chunk_index'] = len(chunks)
                chunks.append(chunk)
                current_chunk_comments = []
                current_size = len(json.dumps(base_post))
            
            current_chunk_comments.append(comment)
            current_size += len(comment_json)
        
        # Flush last chunk
        if current_chunk_comments or not chunks:
            chunk = base_post.copy()
            chunk['comments'] = current_chunk_comments
            chunk['is_chunk'] = True
            chunk['chunk_index'] = len(chunks)
            chunks.append(chunk)
            
        return chunks

if __name__ == "__main__":
    # verification
    import os
    sample_path = 'test_sample.json'
    if os.path.exists(sample_path):
        with open(sample_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        agent = PreprocessAgent()
        if data.get('posts'):
            p = data['posts'][0]
            processed = agent.preprocess_post(p)
            print(json.dumps(processed, indent=2))
