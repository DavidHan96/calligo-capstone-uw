FROM python:3.9

WORKDIR /app
COPY . .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install ipykernel && python -m ipykernel install --name python3 --user

EXPOSE 8050
CMD ["python", "app/main.py"]        
