# Add skip markers for tests that don't work with local LLM/embeddings
import pytest

# Tests that require specific OpenAI behavior
pytestmark = pytest.mark.skipif(
    "LETTA_TEST_LLM_ENDPOINT" in os.environ,
    reason="Test requires OpenAI-specific behavior"
)
