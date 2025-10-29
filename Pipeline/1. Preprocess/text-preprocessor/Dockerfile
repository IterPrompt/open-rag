FROM public.ecr.aws/lambda/python@sha256:5f04a5e231d238774b62a4900bcf7927838811047600583272d590956bbec940 as build

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY utils ${LAMBDA_TASK_ROOT}/utils

CMD [ "lambda_function.handler" ]
