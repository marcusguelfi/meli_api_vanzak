import os

folder = "data/processed"

# lista todos os CSVs no diretório
csv_files = [f for f in os.listdir(folder) if f.endswith(".csv")]

print("📂 CSVs encontrados:")
for f in csv_files:
    print(" -", f)

# define os que você quer manter (os com M)
keep = {"ads_daily.csv", "campaigns_daily.csv", "orders_items_daily.csv"}

to_delete = [f for f in csv_files if f not in keep]

print("\n🧹 Candidatos à exclusão:")
for f in to_delete:
    print(" -", f)

confirm = input("\nDeseja apagar esses arquivos? (s/n): ").strip().lower()
if confirm == "s":
    for f in to_delete:
        path = os.path.join(folder, f)
        try:
            os.remove(path)
            print(f"✅ Removido: {f}")
        except Exception as e:
            print(f"⚠️ Erro ao remover {f}: {e}")
else:
    print("❌ Nenhum arquivo removido.")
