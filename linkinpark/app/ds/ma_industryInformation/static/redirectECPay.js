function go_payment(liff_id) {
    liff.init({
        liffId: liff_id,
        withLoginOnExternalBrowser: true,
    })
    .then(() => {
        idToken = liff.getIDToken()
        window.location = "/ds-manage-assistant-info-web/online-payment/" + idToken + "/" + liff_id
    })}
