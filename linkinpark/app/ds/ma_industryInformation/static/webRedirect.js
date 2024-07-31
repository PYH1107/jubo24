function getToken(liff_id) {
    const isInitExecuted = sessionStorage.getItem("isInitExecuted");
    if (!isInitExecuted) {
        sessionStorage.setItem("isInitExecuted", "true");
        var getOS = liff.getOS();
        liff.init({
            liffId: liff_id,
            withLoginOnExternalBrowser: true,
        })
        .then(() => {
            token = liff.getIDToken()
            sendProfileToServer(token, getOS)
        });
    }
}

function sendProfileToServer(idToken, os) {
    document.getElementById("loading-message").style.display="block";
    fetch ("/ds-manage-assistant-info-web/log_user_profile", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
            token: idToken,
            os: os
        })
    })
    .then(response => response.json())
    .then(data => {
        const userCounty = data.user_county
        if (userCounty){
            document.getElementById("loading-message").innerHTML=`<h1 class="larger-text">資料已取得，正在跳轉至${userCounty}的相關內容</h1>`;
            document.getElementById("table-container").style.display="none";
            window.location.href = `/ds-manage-assistant-info-web/search?county=衛福部&county=線上&county=${userCounty}`;
            document.getElementById("county").value=`${userCounty},衛福部,線上`; 
        } else {
            document.getElementById("table-container").style.display="none";
            window.location.href = `/ds-manage-assistant-info-web/search`
        }
    })
}