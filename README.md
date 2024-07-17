# jubo24
## `.env`
1. 在 `.env` 中新添
    `MONGODB_PASSWORD=password`
2. 在 main.py 中新添
```
# Load environment variables
from dotenv import load_dotenv
load_dotenv()
```