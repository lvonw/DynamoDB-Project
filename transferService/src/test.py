from decimal import Decimal

RED     = "\033[91m"
GREEN   = "\033[92m"
BLUE    = "\033[94m"
CLEAR   = "\033[0m"

# Function for unit testing
def test(expected, result, desc, contains=False, equality=True):
    passed = expected == result if equality else expected != result
    exp_ext = "" if equality else "NOT "
    c = GREEN if passed else RED
    ext = "✅ PASSED! " if passed else "❌ FAILED! "

    print("TESTING: "   + BLUE + desc + CLEAR)
    print("EXPECTED: "  + GREEN + exp_ext + str(expected) + CLEAR) 
    print("GOT: "       + c + str(result))
    print(ext + CLEAR)
    print()

# Main function to run all tests
def run_tests(a4, b4, c4, d4, e4, f4, a5):
    top10_ac = ["Gina Degeneres", 
            "Walter Torn", 
            "Mary Keitel", 
            "Matthew Carrey", 
            "Sandra Kilmer", 
            "Scarlett Damon", 
            "Groucho Dunst", 
            "Uma Wood", 
            "Angela Witherspoon", 
            "Vivien Basinger"]

    top10_cust_rental = ["CST#148",
                        "CST#526",
                        "CST#144",
                        "CST#236",
                        "CST#75",
                        "CST#197",
                        "CST#469",
                        "CST#137",
                        "CST#178",
                        "CST#468"]

    top10_cust_pay = []

    prev_pwds = ["8cb2237d0679ca88db6464eac60da96345513964",
                "8cb2237d0679ca88db6464eac60da96345513964"]


    print("========== TEST ==============================================")
    # 4
    test(4581, a4, 
        "Gesamtanzahl der verfügbaren Filme")
    test((759, 762), b4, 
        "Anzahl der Unterschiedlichen Filme je Standort")
    test(top10_ac, c4, 
        """Die Vor- und Nachnamen der 10 Schauspieler mit den meisten 
        Filmen, absteigend sortiert""")
    test({"STF#1": Decimal("30252.12"), "STF#2": Decimal("31059.92")}, d4, 
        """Die Erlöse je Mitarbeiter """)
    test(top10_cust_rental, e4, 
        """Die IDs der 10 Kunden mit den meisten Entleihungen """)
    test(top10_cust_pay, f4, 
        """Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, 
        die das meiste Geld ausgegeben haben """)

    #5
    test(prev_pwds, a5, 
        """Vergebt allen Mitarbeitern ein neues, sicheres Passwort """, 
        equality=False)

