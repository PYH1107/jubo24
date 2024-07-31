Blood sugar anomaly detection
---

Boot the server
```sh
python3 linkinpark/app/ai/bloodsugar/server.py
```

I/O format
```
[
    {"patient": <string>, "sugar_type": <"AC"/"PC">, "sugar_value": <int>}
]
```

Example request:
```
[
    {"patient": "5ce3d00b401076069ac8c13f", "sugar_type": "AC", "sugar_value": 200}, 
    {"patient": "5ce3d00b401076069ac8c13f", "sugar_type": "PC", "sugar_value": 200},
    {"patient": "5ce3d00b401076069ac8c13f", "sugar_type": "AC", "sugar_value": 100},
    {"patient": "5ce3d00b401076069ac8c13f", "sugar_type": "PC", "sugar_value": 100}
]
```

