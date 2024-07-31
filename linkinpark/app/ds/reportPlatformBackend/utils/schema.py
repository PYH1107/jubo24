google_workspace_file_type = {
    "application/vnd.google-apps.document":
        "application/vnd.openxmlformats-officedocument.wordprocessingml"
        ".document",
    "application/vnd.google-apps.presentation":
        "application/vnd.openxmlformats-officedocument.presentationml"
        ".presentation",
    "application/vnd.google-apps.spreadsheet":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.google-apps.drawing": "image/jpeg",
}
mime_type = {
    "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml"
             ".sheet",
    "word": "application/vnd.openxmlformats-officedocument.wordprocessingml"
            ".document",
    "PowerPoint": "application/vnd.openxmlformats-officedocument"
                  ".presentationml.presentation",
    "pdf": "application/pdf",
}
gender = {
    "male": "男性",
    "female": "女性",
}
tube_material = {
    "SI": "矽質",
    "normal": "一般材質",
}
tube_type = {
    "NG": "鼻胃管",
    "PEG": "胃造廔管",
    "foley": "導尿管",
    "tracheostomy": "氣切管",
    "cystostomy": "膀胱造瘻口",
    "enterostomy": "肛門造瘻口",
}
tube_remove_reason = {
    "dead": "死亡",
    "remove": "成功移除",
    "hospital": "住院",
    "away": "請假",
    "unpresented": "退住",
}
assist_status = {
    "self": "自行完成",
    "needAssistance": "部分協助",
    "fullAssistance": "完全協助",
}
general_status = {
    "good": "佳",
    "bad": "不佳",
    "normal": "普通",
}
emotion_status = {
    "happy": "開心",
    "calm": "平靜",
    "moody": "憂鬱",
    "uneasy": "躁動",
    "depressed": "不舒服",
}
food_type = {
    "normal": "普通",
    "chopped": "碎食",
    "soft": "軟食",
    "bring": "自備",
    "others": "特殊",
}
ate_amount = {
    "all": "全部",
    "oneThird": "不足一半1/3",
    "half": "一半1/2",
    "most": "大部分2/3",
    "refuse": "拒絕",
}
drink_amount = {
    'below': "500c.c.以下",
    'moderate': "500至1000c.c.",
    'above': "1000c.c.以上",
}
sleep_time = {
    'aboveAHalf': "一個半小時以上",
    'inAHour': "一小時內",
    'none': "無午休",
    'thirtyMinutes': "三十分鐘",
}
sleep_type = {
    'lie': "躺床",
    'sit': "坐椅",
    'roam': "遊走",
}
yes_no_type = {
    "yes": "有",
    "no": "無",
}
change_dress = {
    "cloth": "衣服",
    "pants": "褲子",
    "clothAndPants": "衣服及褲子",
}
take_drug_method = {
    "self": "自行服用",
    "aid": "協助服用",
    "notTook": "未服用",
    "no": "沒有藥物",
}
urination_color = {
    "brown": "茶褐色",
    "loghtRed": "淡紅色",
    "red": "紅色",
    "yellow": "黃色",
}
defecation_amount = {
    "learge": "大量",
    "medium": "中等",
    "small": "少量",
}
defecation_type = {
    "hardGrain": "顆粒硬便",
    "hardStrip": "條狀硬便",
    "softStrip": "條狀軟便",
    "watery": "稀水便",
}

pain_frequency = {
    'continue': '持續',
    'increase': '增加',
    'intermittent': '間歇'
}
pain_assessmentType = {
    'dementia': '失智症'
}
pain_cause = {
    'bedSores': '褥瘡',
    'cancer': '癌症',
    'chemicalInjury': '化學性損傷',
    'diseaseFactor': '疾病因素',
    'falldown': '跌倒',
    'fracture': '骨折',
    'gout': '痛風',
    'heartPain': '心痛',
    'hotBurns': '燒燙傷',
    'inflammation': '發炎',
    'muscleDamage': '肌肉損傷',
    'neuropathicPain': '神經性疼痛',
    'osteoarthritis': '關節炎',
    'spasmPain': '痙攣痛',
    'surgery': '手術',
    'trauma': '外傷',
    'tumor': '腫瘤'
}
pain_intensify = {
    'bodyChange': '姿勢改變',
    'climateChange': '天氣變化',
    'cold': '冷',
    'cough': '咳嗽',
    'insomnia': '失眠',
    'move': '活動',
    'moveArea': '活動',
    'overwork': '過度工作',
    'staybed': '臥床',
    'strenuousExercise': '劇烈運動',
    'touch': '觸摸'
}
pain_nature = {
    'angina': '心絞痛',
    'beating': '敲通',
    'cannotExpress': '無法表達',
    'fullnessPain': '脹痛',
    'numbness': '隱隱作痛',
    'obtusePain': '鈍痛',
    'soreness': '痠痛',
    'stinging': '刺痛',
    'tearingPain': '撕裂痛',
    'touch': '觸痛',
    'twitch': '抽痛',
}
pain_degree = {
    'deep': '深層',
    'surface': '表皮',
    'both': '深層/表皮',
    'unassessable': '無法評估'
}
pain_diffusion = {
    'no': '否',
    'yes': '是'
}
pain_effect = {
    'activityReduced': '活動量下降',
    'appetiteChange': '食慾改變',
    'emotionalChange': '情緒改變',
    'insomnia': '失眠'
}
pain_result = {
    'improvement': '改善',
    'intensify': '加重',
    'noChange': '沒有改變',
    'painRelief': '緩解',
    'shortTermPain': '短暫疼痛'
}
pain_state = {
    'finished': '已完成',
    'unfinished': '未完成'
}

pain_process = {
    "rest": "臥床休息",
    "hotBath": "熱水澡",
    "massage": "按摩",
    "ointment": "塗抹藥膏",
    "westernMedicine": "口服西藥",
    "chineseMedicine": "口服中藥",
    "hotPack": "熱敷",
    "coldPack": "冷敷",
    "touch": "觸摸",
    "chineseMassage": "中醫推拿",
    "electrotherapy": "電療",
    "release": "放送心情",
    "injection": "注射針劑",
}
