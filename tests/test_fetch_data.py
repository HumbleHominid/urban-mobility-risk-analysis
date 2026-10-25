from urban_mobility.fetch_data import get_municipality_population

df = get_municipality_population().set_index(["municipality", "UJAHR"])
for year, pop in {2016: 33633, 2025: 34822}.items():
    row = df.loc[("05566084", year)]
    assert (row["name"], row["kind"], row["population"]) == ("Steinfurt", 63, pop), row
assert (df.index.get_level_values("UJAHR") == 2025).sum() == 10953
print("ok")
