from collections import Counter


def get_prior(dataset):
    prior = dataset.groupby('title').size().div(len(dataset))  # count()['Age']/len(data)
    return prior


def get_likelihood(dataset, prior, features):
    likelihood = {}
    for key, feature in features.items():
        try:
            likelihood[key] = dataset.groupby(['title', key]).size().div(len(dataset)).div(prior)
        except KeyError:
            print(key, feature)

    return likelihood


def get_posterior(title, prior, likelihood, features):
    if title in prior:
        posterior = prior[title]
    else:
        return 0
    for key, feature in features.items():
        feature = int(feature)
        try:
            posterior = posterior * likelihood[key][title][feature]
        except Exception as e:
            posterior = posterior * 0.0001

    return posterior


def get_rule_based(rb, rbt, features):
    rb_list = []
    rbt_list = []
    for col, value in features.items():
        try:
            if isinstance(value, list):
                for item in value:
                    for num in rb[col][item]:
                        if num not in rb_list:
                            rb_list.append(num)

            else:
                for num in rb[col][value]:
                    if num not in rb_list:
                        rb_list.append(num)
        except Exception as e:
            print(f"Error for {col}, {value}", flush=True)

    for num in rb_list:
        rbt_list.append(rbt[str(num)])
    return rbt_list


def risk_analysis(dataset, titles, features):
    isNormal = False
    titles_list = list(titles.keys())

    prior = get_prior(dataset)
    likelihood = get_likelihood(dataset, prior, features)

    total_posterior = {}
    for title in titles_list:
        posterior = get_posterior(title, prior, likelihood, features)
        total_posterior[title] = posterior

    total_posterior_5 = dict(Counter(total_posterior).most_common(5))

    duplicate = 0
    if "皮膚完整性受損" in total_posterior_5.keys() and "潛在危險性皮膚完整性受損" in total_posterior_5.keys():
        duplicate += 1
    if "潛在危險性感染 - 呼吸道" in total_posterior_5.keys() and "現存性感染-呼吸道" in total_posterior_5.keys():
        duplicate += 1
    if "潛在危險性感染 - 尿道" in total_posterior_5.keys() and "現存性感染-尿道" in total_posterior_5.keys():
        duplicate += 1
    if "潛在危險性跌倒" in total_posterior_5.keys() and "跌倒" in total_posterior_5.keys():
        duplicate += 1
    if "營養狀況改變 - 少於身體所需" in total_posterior_5.keys() and "營養狀況改變 - 多於身體所需" in total_posterior_5.keys():
        duplicate += 1
    if '正常' in total_posterior_5.keys():
        duplicate += 1

    pairList = [("皮膚完整性受損", "潛在危險性皮膚完整性受損"), ("現存性感染-呼吸道", "潛在危險性感染 - 呼吸道"),
                ("潛在危險性感染 - 尿道", "現存性感染-尿道"), ("跌倒", "潛在危險性跌倒")]
    compareList = [("營養狀況改變 - 少於身體所需", "營養狀況改變 - 多於身體所需")]

    if duplicate != 0:
        total_posterior_all = dict(Counter(total_posterior).most_common(5 + duplicate))
        for item in pairList:
            if item[0] in total_posterior_all.keys() and item[1] in total_posterior_all.keys():
                total_posterior_all.pop(item[1])
        for item in compareList:
            if item[0] in total_posterior_all.keys() and item[1] in total_posterior_all.keys():
                if total_posterior_all[item[0]] > total_posterior_all[item[1]]:
                    total_posterior_all.pop(item[1])
                else:
                    total_posterior_all.pop(item[0])

        if '正常' in total_posterior_all.keys():
            isNormal = True
            total_posterior_all.pop('正常')
    else:
        return total_posterior_5, isNormal

    return total_posterior_all, isNormal
