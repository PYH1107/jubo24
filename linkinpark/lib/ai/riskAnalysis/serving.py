from datetime import datetime, timezone, timedelta
from linkinpark.lib.ai.riskAnalysis import utils
from linkinpark.lib.common.gcs_helper import GcsHelper
from linkinpark.lib.common import mongo_connector

GCS_BUCKET = "jubo-ai-serving"
GCS_BLOB_PREFIX = "ai-riskanalysis/"


class ServingRiskAnalysis:
    """
    Functions
    ----------
    predict : arrange data > forward to bayes algorithm > re-order result > adjust content
    data_arrange : arrange incoming data to binary format
    store_output : store output to aimodel mongoDB
    -------
    """
    def __init__(self):
        """
        initial download following config file from gcs
        care_plan_diagnoses_homeangel_with_normal_220901: training data
        output_nursing_diagnoses_Vorigin: risk label
        phyassts_schema_ha: homeAngel phyassts problems schema
        rule-based_v2_220325: rule-based phyassts problem to number
        rule-based-transform: transform number to risk
        diagnose2general: homeAngel custom title
        """
        self.gcs = GcsHelper()

        self.dataset = self.gcs.download_by_string_from_bucket(GCS_BUCKET,
                                                               f"{GCS_BLOB_PREFIX}care_plan_diagnoses_homeangel_with_normal_220901.pkl")
        self.titles = self.gcs.download_by_string_from_bucket(GCS_BUCKET,
                                                              f"{GCS_BLOB_PREFIX}output_nursing_diagnoses_Vorigin.json")
        self.ha_col = self.gcs.download_by_string_from_bucket(GCS_BUCKET, f"{GCS_BLOB_PREFIX}phyassts_schema_ha.json")
        self.rb = self.gcs.download_by_string_from_bucket(GCS_BUCKET, f"{GCS_BLOB_PREFIX}rule-based_v2_220325.json")
        self.rbt = self.gcs.download_by_string_from_bucket(GCS_BUCKET, f"{GCS_BLOB_PREFIX}rule-based-transform.json")
        self.diagnose2general = self.gcs.download_by_string_from_bucket(GCS_BUCKET,
                                                                        f"{GCS_BLOB_PREFIX}diagnose2general.json")

    def predict(self, data):
        features, patient_data = self.data_arrange(data)

        total_posterior, isNormal = utils.risk_analysis(self.dataset, self.titles, features)
        rbt_list = utils.get_rule_based(self.rb, self.rbt, data)
        n_items = total_posterior

        total_np = sum([prob for x, prob in n_items.items()])
        result = [[title, prob / total_np] for title, prob in n_items.items()]

        # re-order result : check rule-based and move to front index of an array
        reorder = []
        for re in result:
            if re[0] in rbt_list:
                reorder.append(re)

        for i in result:
            if i not in reorder:
                reorder.append(i)
        new_result = []
        for array in reorder:
            for k, v in self.diagnose2general.items():
                if array[0] in v:
                    new_result.append([k, array[1]])

        # adjust content: if normal, rewrite all to '低風險'
        if isNormal:
            for i, item in enumerate(new_result):
                item[1] = "低風險"
        else:
            for i, item in enumerate(new_result):
                if i == 0:
                    item[1] = "高風險"
                elif i in [1, 2, 3]:
                    item[1] = "中風險"
                else:
                    item[1] = "低風險"

        self.store_output(data, result, patient_data)

        return new_result

    def data_arrange(self, data):
        """
        act_cond, eval, workremark: personal info. from homeAngel
        """
        new_data = {}
        patient_info = {}

        for col, value in data.items():
            if col in ['act_cond', 'eval', 'workremark']:
                patient_info[col] = value
            try:
                if self.ha_col['phyassts']['content'][col]['type'] == 'array':
                    if isinstance(value, list):
                        for feature in value:
                            new_data[col + '_' + feature] = '1'
                    else:
                        new_data[col + '_' + value] = '1'
                else:
                    new_data[col + '_' + value] = '1'
            except KeyError:
                print(col, value)
                continue

        return new_data, patient_info

    def store_output(self, data, output, patient_data):
        db = mongo_connector.MongodbAI()

        dt_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        dt_tw = dt_utc.astimezone(timezone(timedelta(hours=8)))

        col = db['aiRiskAnalysis_HomeAngel']
        col.insert_one({'features': data, "output": output, 'patientInfo': patient_data, 'createdDate': dt_tw})
