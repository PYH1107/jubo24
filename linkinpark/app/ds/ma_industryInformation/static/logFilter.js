var tagSelect = document.getElementById("tag");
var countySelect = document.getElementById("county");

tagSelect.addEventListener("change", updateSelectionText);
countySelect.addEventListener("change", updateSelectionText);

function updateSelectionText() {
    var tagValue = tagSelect.value;
    var countyValue = countySelect.value;
    document.getElementById("selected-choices").innerText = "選擇的類別：" + tagValue + "，縣市：" + countyValue;
}

document.getElementById("selection-form").addEventListener("submit", function (event) {
    event.preventDefault();
    saveSelectionToStorage();
    var multipleTag = tagSelect.value;
    var tagValue = multipleTag.split(",");
    var multipleCounty = countySelect.value;
    var countyValue = multipleCounty.split(",");

    var url = "/ds-manage-assistant-info-web/search?";
    if (tagValue.length > 0 && countyValue.length > 0) {
        for (var i = 0; i < tagValue.length; i++) {
            url = url + "tag=" + tagValue[i];
            if (i < tagValue.length - 1) {
                url = url + "&";
                }
            };
        for (var i = 0; i < countyValue.length; i++) {
            url = url + "&county=" + countyValue[i];
            if (i < countyValue.length - 1) {
                url = url + "&";
                }
            }
        }
    else if (tagValue && tagValue.length > 0) {
        for (var i=0; i < countyValue; i++) {
            url = url + "county=" +countyValue[i];
            if (i < countyValue.length - 1) {
                url = url + "&";
                }
            }
        }
    else if (countyValue && countyValue.length > 0) {
        for (var i = 0; i < countyValue.length; i++) {
            url = url + "county=" + countyValue[i];
        if (i < countyValue.length - 1) {
            url = url + "&";
            }
        }
    }
    window.location.href = url;
});

function saveSelectionToStorage() {
    var tagSelect = document.getElementById("tag");
    var countySelect = document.getElementById("county");

    sessionStorage.setItem("selectedTag", tagSelect.value);
    sessionStorage.setItem("selectedCounty", countySelect.value);
};

function loadSelectionFromStorage() {
    var tagSelect = document.getElementById("tag");
    var countySelect = document.getElementById("county");

    var selectedTag = sessionStorage.getItem("selectedTag");
    var selectedCounty = sessionStorage.getItem("selectedCounty");

    if (selectedTag) {
        tagSelect.value = selectedTag;
    }

    if (selectedCounty) {
        countySelect.value = selectedCounty;
    }

    updateSelectionText();
};
window.addEventListener("load", function () {
    loadSelectionFromStorage();
    updateSelectionText();
});