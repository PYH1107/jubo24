# jubo24
## `.env`
1. 在 `.env` 中新添
    `MONGODB_PASSWORD=password`
2. 在 main.py 中新添
```python
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# update:
主要更新在 `pmain.py`
```