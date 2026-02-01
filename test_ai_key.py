#!/usr/bin/env python3
"""
Test script to verify AI_KEY works with OpenRouter.
Usage: python test_ai_key.py
"""
import os
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "google/gemini-2.5-flash"


def test_ai_key():
    print("=" * 60)
    print("AI_KEY Test Script")
    print("=" * 60)
    
    # Check if AI_KEY is set
    api_key = os.getenv("AI_KEY")
    
    if not api_key:
        print("❌ FAILED: AI_KEY is not set in environment")
        print("\nTo fix:")
        print("1. Copy .env.example to .env")
        print("2. Add your OpenRouter API key to AI_KEY in .env")
        print("3. Run this script again")
        return False
    
    api_key = api_key.strip()
    print(f"✓ AI_KEY found (length: {len(api_key)})")
    
    # Test the key with OpenRouter
    print(f"\nTesting key with OpenRouter ({DEFAULT_MODEL})...")
    
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": DEFAULT_MODEL,
                    "messages": [
                        {"role": "user", "content": "Say 'Hello, AI is working!'"}
                    ],
                    "max_tokens": 50,
                },
            )
            
            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    message = result["choices"][0].get("message", {}).get("content", "")
                    print(f"\n✅ SUCCESS! API key is active and working.")
                    print(f"\nModel response: {message}")
                    print(f"\nModel used: {result.get('model', 'unknown')}")
                    return True
                else:
                    print(f"❌ Unexpected response format: {result}")
                    return False
            else:
                print(f"❌ FAILED: HTTP {response.status_code}")
                print(f"\nResponse: {response.text[:500]}")
                
                if response.status_code == 401:
                    print("\n⚠️  Authentication failed. Check:")
                    print("  - API key is correct (get one at https://openrouter.ai/keys)")
                    print("  - Key starts with 'sk-or-v1-'")
                elif response.status_code == 429:
                    print("\n⚠️  Rate limit or quota exceeded")
                elif response.status_code >= 500:
                    print("\n⚠️  OpenRouter server error (try again later)")
                
                return False
                
    except httpx.TimeoutException:
        print("❌ FAILED: Request timed out")
        print("Check your internet connection")
        return False
    except Exception as e:
        print(f"❌ FAILED: {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    success = test_ai_key()
    print("\n" + "=" * 60)
    if success:
        print("✅ Test PASSED - Your AI_KEY is working!")
        print("\nYou can now use this key in:")
        print("  - Local: .env file")
        print("  - Railway: Variables tab (AI_KEY)")
    else:
        print("❌ Test FAILED - Fix the issues above and try again")
    print("=" * 60)
