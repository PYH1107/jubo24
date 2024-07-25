from datetime import datetime, timedelta
relative_date = "大前天"
def relative_date_to_absolute(relative_date):
    today = datetime.today()
    if relative_date == "今天":
        return today.strftime("%Y-%m-%d")
    elif relative_date == "昨天" or relative_date == "昨日":
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    elif relative_date == "大前天" or relative_date == "大前日":
        print("大前天")
        return (today - timedelta(days=3)).strftime("%Y-%m-%d")
    elif relative_date == "前天" or relative_date == "前日":
        print("前天")
        return (today - timedelta(days=2)).strftime("%Y-%m-%d")
    else:
        return None

dates = []
date_patterns = [
        r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b',  # MM-DD-YYYY or MM/DD/YYYY
        r'\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b',  # YYYY-MM-DD or YYYY/MM/DD
        r'\b(\d{4})年(\d{1,2})月(\d{1,2})日\b',  # YYYY年MM月DD日
        r'\b(\d{1,2})月(\d{1,2})日(\d{4})年\b',  # MM月DD日YYYY年
        r'\b民國(\d{1,3})年(\d{1,2})月(\d{1,2})日\b',  # 民國YYY年MM月DD日
        r'\b(\d{2,3})[-/](\d{1,2})[-/](\d{1,2})\b',  # YYY-MM-DD or YYY/MM/DD (民國年)
]
for pattern in date_patterns:
    matches = re.finditer(pattern, text)
    for match in matches:
        try:
            groups = match.groups()
            if len(groups) == 3:
                if '年' in pattern or '月' in pattern or '日' in pattern:
                    if '民國' in pattern:
                        year = int(groups[0]) + 1911
                    else:
                            year = int(groups[0])
                        month = int(groups[1])
                        day = int(groups[2])
                elif len(groups[0]) == 4:  # YYYY-MM-DD
                    year, month, day = map(int, groups)
                elif len(groups[2]) == 4:  # MM-DD-YYYY
                        month, day, year = map(int, groups)
                else:  # YYY-MM-DD (民國年)
                    year = int(groups[0]) + 1911
                    month, day = map(int, groups[1:])

                if 1 <= month <= 12 and 1 <= day <= 31:
                    if year < 1911:  # 處理可能的民國年份
                        year += 1911
                    parsed_date = datetime(year, month, day)
                    formatted_date = parsed_date.strftime('%Y-%m-%d')
                    if formatted_date not in dates:
                        dates.append(formatted_date)
        except ValueError:
            # 如果日期無效，跳過
            continue

    # 處理相對日期
relative_dates = ["今天", "昨天", "昨日", "前天", "前日", "大前天", "大前日"]
for rel_date in relative_dates:
    if rel_date in text:
        abs_date = relative_date_to_absolute(rel_date)
        if abs_date and abs_date not in dates:
            dates.append(abs_date)

dates = sorted(dates)  # 按日期排序
if len(dates) == 1:
    from_date = dates[0]
    to_date = dates[0]
    dates = [from_date, to_date]
elif len(dates) > 1:
    from_date = dates[0]
    to_date = dates[-1]
    dates = [from_date, to_date]


result = relative_date_to_absolute(relative_date)