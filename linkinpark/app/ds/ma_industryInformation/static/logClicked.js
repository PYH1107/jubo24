function logLabelClick() {
    var currentTime = new Date().toLocaleString();
    var tagValue = tagSelect.value;
    var countyValue = countySelect.value;
    var userLabelData = {
        user_id: profile.userId,
        click_time: currentTime,
        tag: tagValue,
        county: countyValue
    };
    fetch("/ds-manage-assistant-info-web/log_user_label", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(userLabelData),
    })
}