function getToken(liff_id) {
    liff.init({
        liffId: liff_id,
        withLoginOnExternalBrowser: true,
    })
    .then(() => {
        token = liff.getIDToken();
        sendProfileToServer(token)
    })}

function sendProfileToServer(idToken) {
    fetch ("/ds-manage-assistant-info-web/log_user_profile", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({token: idToken})})}
