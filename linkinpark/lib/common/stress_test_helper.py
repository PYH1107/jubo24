import os
import json
import pandas as pd


class StressTestHelper():
    '''
    An interface to test whether your API tolerable as huge request coming.
    According to the given concurrency_list, check whether the latency under different concurrency passes the limited time.

    Parametes
    ---------
    latency_limit_ms : int, default=1000
        The maximum latency that the API can tolerate.

    Attributes
    ----------
    check_api_tolerable_in_concurrency_list:
    - Input :
        - concurrency_list : list of int
            A sequence of concurrency number.
        - api_url : str
            API's URL.
        - request_method : str
            request method, supported for GET, POST and PUT.
        - input_dict : dict
            API's input data, necessary if using method POST / GET
        - content_type : str, default='application/json'
            necessary if using method POST / GET
        - strategy : str, default='maximum'
            supported 'maximum' and 'average'
    - Output :
        - concurrency_latency_output : dict
            Each concurrency latency time.
        - passed : bool
            Whether the API tolerate on all conditions.

    Examples
    --------
    >>> from linkinpark.lib.common.stress_test_helper import StressTestHelper
    >>> api_url = '0.0.0.0:8000/bmi'
    >>> method = 'POST'
    >>> input_dict = {"height": 208, "weight": 120}
    >>> api_tester = StressTestHelper(latency_limit_ms=1000)
    >>> concurrency_latency_output, passed = apt_tester.check_api_tolerable_in_concurrency_list(concurrency_list=[5, 10, 50, 100, 1000],
    ...     input_dict=input_dict, api_url=api_url, method=method)
    '''
    def __init__(self, latency_limit_ms=1000):
        self.latency_limit_ms = latency_limit_ms
        self.dirname = os.path.abspath(os.path.dirname(__file__))

    def check_api_tolerable_in_concurrency_list(self, concurrency_list, api_url, request_method, input_dict=None, content_type='application/json', strategy='maximum'):
        self.__check_method_and_body(request_method, input_dict)

        concurrency_latency_output = {}

        num_requests = max(concurrency_list)
        for concurrency in concurrency_list:
            output_file = os.path.join(self.dirname, f'ab_concurrency_{concurrency}.csv')
            if request_method != 'GET':
                print(f'ab -k -n {num_requests} -c {concurrency} -q -g {output_file} -T {content_type} -p {self.post_file} {api_url} > /dev/null')
                os.system(f'ab -k -n {num_requests} -c {concurrency} -q -g {output_file} -T {content_type} -p {self.post_file} {api_url} > /dev/null')
            else:
                print(f'ab -k -n {num_requests} -c {concurrency} -q -g {output_file} -T {content_type} {api_url} > /dev/null')
                os.system(f'ab -k -n {num_requests} -c {concurrency} -q -g {output_file} -T {content_type} {api_url} > /dev/null')
            latency = self.__analysis_concurrency_latency(f'{output_file}', strategy)
            print(f'=== {strategy} latency of concurrency {concurrency} : {latency} ms')

            concurrency_latency_output[concurrency] = latency

        passed = self.__check_concurrency_latency_output_passed(concurrency_latency_output)
        if request_method != 'GET':
            self.__clean_post_file()

        return concurrency_latency_output, passed

    def __check_method_and_body(self, request_method, input_dict):
        assert request_method in ['GET', 'POST', 'GET'], 'Supported only GET, POST and PUT request method.'

        if request_method != 'GET':
            assert input_dict, 'Necessary to pass an input_dict.'
            self.__convert_input_data_as_post_file(input_dict)

    def __convert_input_data_as_post_file(self, input_dict):
        with open(os.path.join(self.dirname, "input_dict.json"), "w") as f:
            json.dump(input_dict, f)
        self.post_file = os.path.join(self.dirname, "input_dict.json")

    def __analysis_concurrency_latency(self, output_file, strategy):
        ab_output = pd.read_csv(output_file, sep='\t')
        os.remove(output_file)

        if strategy == 'maximum':
            return ab_output['ttime'].max()
        elif strategy == 'average':
            return ab_output['ttime'].mean()

    def __check_concurrency_latency_output_passed(self, concurrency_latency_output):
        for concurrency in concurrency_latency_output:
            if concurrency_latency_output[concurrency] > self.latency_limit_ms:
                failed_message = f"API not passed in {concurrency} concurrency ({concurrency_latency_output[concurrency]} ms > {self.latency_limit_ms} ms)"
                print(failed_message)
                return False
        return True

    def __clean_post_file(self):
        os.remove(self.post_file)
