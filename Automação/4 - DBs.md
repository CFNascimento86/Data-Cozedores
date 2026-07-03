#### 🧱 1. Princípio fundamental: 1 DB por Cozedor

Com 10 cozedores, a melhor prática é:

DB101 – Cozedor 01  
DB102 – Cozedor 02  
DB103 – Cozedor 03  
DB104 – Cozedor 04  
DB105 – Cozedor 05  
DB201 – Cozedor 06  
DB202 – Cozedor 07  
DB203 – Cozedor 08  
DB204 – Cozedor 09  
DB205 – Cozedor 10  

Por que isso é excelente?
- facilita leitura via Snap7 (um DB = um bloco de bytes)
- facilita manutenção no TIA/Step7
- evita offsets gigantescos
- permite paralelismo no Edge
- facilita particionamento no SQL Server
- deixa o pipeline extremamente organizado

---

#### 🧩 2. Estrutura interna do DB (padrão Siemens para leitura via Snap7)

Cada DB deve ter sempre a mesma estrutura, com offsets fixos.
Aqui está o modelo recomendado:
````
STRUCT
    Temperatura          : REAL;   // °C
    Pressao\Vacuo         : REAL;   // kPa ou mmHg
    Brix                 : REAL;   // %
    Nivel                : REAL;   // %
    Vazao_Vapor           : REAL;   // t/h ou kg/h
    Vazao_Alimentacao     : REAL;   // t/h
    Estado_Batelada       : INT;    // 0=Parado, 1=Carregando, 2=Cozendo, 3=Descarga
    Pureza               : REAL;   // %
    Condensado           : REAL;   // °C ou kg/h
END_STRUCT
````
Total: 34 bytes por DB

Isso é perfeito para Snap7.

---

🧠 3. Por que REAL + INT?

- Snap7 lê REAL e INT de forma nativa
- evita conversões complexas
- garante compatibilidade com SQL Server
- mantém precisão industrial
