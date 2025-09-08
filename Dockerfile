# Usa imagem base do Python
FROM python:3.11-slim

# Define diretório de trabalho
WORKDIR /app

# Copia requirements e instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código da aplicação
COPY . .

# Comando padrão para rodar
CMD ["python", "atualizacao_de_pedidos.py"]