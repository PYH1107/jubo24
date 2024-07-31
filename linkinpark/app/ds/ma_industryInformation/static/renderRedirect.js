function handleClick(event, liff_id, titleText, linkText, tagText) {
    event.preventDefault();
    logLinkClick(liff_id, titleText, linkText, tagText);
    window.open(linkText, '_blank');
};
function logLinkClick(liff_id, titleText, linkText, tagText) {
    var currentTime = new Date().toISOString();

    liff.init({
        liffId: liff_id,
        withLoginOnExternalBrowser: true,
    })
    .then(() => {
        token = liff.getIDToken();
        var dataToSend = {
            token: token,
            click_time: currentTime,
            title: titleText,
            link: linkText,
            tag: tagText
        };
        return sendLinkToServer(dataToSend);
    })
};
function sendLinkToServer(data) {
    return fetch ("/ds-manage-assistant-info-web/log_user_link", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(data)
    })
    .then(response => {
        return response.json();
    })
};
function scrollToTop() {
    window.scrollTo({ top: 0, behavior: "smooth"})
};