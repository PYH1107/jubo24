from setuptools import find_packages, setup

__version__ = None
package_name = 'linkinpark'

setup(
    name=package_name,
    version=__version__,
    author="Jubo AI Team",
    packages=find_packages(),
    entry_points={'console_scripts': [
        'lpsql = linkinpark.script.common.execute_bq_command:main',
        'run-ai-activity-recommendation = linkinpark.app.ai.activityRecommendationNN.server:main',
        'run-ai-mental-abnormal-detection = linkinpark.app.ai.mentalAbnormalNN.server:main',
        'run-ai-vitalsign = linkinpark.app.ai.vitalsign.server:main',
        'run-ai-voicefill = linkinpark.app.ai.voicefill.server:main',
        'run-ai-faceVitalsign = linkinpark.app.ai.faceVitalsign.server:main',
        'run-ai-patientKG = linkinpark.app.ai.patientKG.server:main',
        'run-ai-bloodsugar = linkinpark.app.ai.bloodsugar.server:main',
        'run-ai-riskAnalysis = linkinpark.app.ai.riskAnalysis.server:main',
        'run-ai-focusKG = linkinpark.app.ai.focusKG.server:main',
        'run-ai-llm-agent-feedback = linkinpark.app.ai.feedback.server:main',
        'run-ds-report-platform = linkinpark.app.ds.reportPlatformBackend.server:main',
        'run-ds-manage-assistant-info-web = linkinpark.app.ds.ma_industryInformation.server:main',
        'run-login-server = linkinpark.app.infra.ai_login_page.server:main',
        'run-sev-server = linkinpark.app.infra.aids_sev_server.app:main',
        'run-token-server = linkinpark.app.infra.ai_token_server.server:main',
        'run-pdf2image = linkinpark.app.infra.pdf2image.app:main',
        ['run-ai-patientdisease = linkinpark.app.ai.patientDisease.server:main'],]
    },
    include_package_data=True,
    package_data={
        # If any package contains *.<ext>, include them:
        '': [
            '*.sql',
            'templates/*.html',
            'static/*.css',
            'static/*.js',
            'static/css/*.css',
            'static/js/*.js',
            '*.json',
            '*.html',
            '*.md',
            '*.css',
            '*.png',
            '*.xlsx'
        ],
    },
    install_requires=[
        'numpy==1.21.5',
        'easydict==1.9',
        'pandas==1.3.5',
        'pymongo==3.10.1',
        'pymongo[srv]',
        'tqdm==4.62.3',
        'google-cloud-pubsub',
        'jinja2==3.0.1',
        'jinjasql==0.1.8',
        'google-cloud-bigquery==2.34.1',
        'google-cloud-storage',
        'google-auth-oauthlib',
        'google-cloud-secret-manager',
        'pyarrow',
        'PyYAML',
        'requests>=2.28.1',
        'pyod',
        'pmdarima',
        'sktime',
        'pymssql==2.2.8',
        'psycopg2-binary',
        'google-cloud-logging',
        # used for lib.ds.sharedrive_connector
        'google-api-python-client==2.50.0',
        'openpyxl==3.0.10',
        # used for lib.ds.tableau_connector
        'tableau-api-lib==0.1.24',
        # used for linkinpark/app
        'Flask==2.0.2',
        'prometheus-client==0.12.0',
        'Werkzeug==2.0.3',
        # used for app.ai_login_page
        'authlib',
        'six',
        # used for app.ai.vitalsign
        'shap==0.41.0',
        'scikit-learn==1.0.2',
        # used for fastapi & monitor
        'fastapi==0.103.2',
        'pydantic>=1.10.2',
        'starlette==0.27.0',
        'starlette_prometheus==0.9.0',
        'asgiref==3.5.2',
        'uvicorn==0.20.0',
        'PyJWT==2.6.0',
        'psutil',
        'httpx',
        # used for app.ai.patientdisease
        'optuna',
        'lightgbm',
        # Used for app.ds.reportPlatform
        'cython==0.29.32',
        'google-cloud-core==2.3.2',
        'Pillow==9.2.0',
        'PyPDF2==1.26.0',
        'python-dateutil==2.8.2',
        'xlrd==2.0.1',
        'pytz~=2022.2.1',
        # used for app.ai.voicefill
        'wave',
        'python-multipart==0.0.6',
        # used for app.ds.ma_industryInformation
        'Markdown==3.4.4',
    ],
)
