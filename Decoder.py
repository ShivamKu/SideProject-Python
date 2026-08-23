import requests
from bs4 import BeautifulSoup


def decode_secret_message(url):
    response = requests.get(url)
    response.raise_for_status()

    print("HTTP status:", response.status_code)
    print("HTML length:", len(response.text))

    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("table")
    rows = soup.find_all("tr")

    print("Tables found:", len(tables))
    print("Rows found:", len(rows))

    points = []

    for index, row in enumerate(rows):
        cells = row.find_all("td")

        # Diagnostic output for first few rows
        if index < 6:
            print(
                "ROW",
                index,
                "=>",
                [cell.get_text(strip=True) for cell in cells]
            )

        if len(cells) != 3:
            continue

        x_text = cells[0].get_text(strip=True)
        char = cells[1].get_text(strip=True)
        y_text = cells[2].get_text(strip=True)

        try:
            x = int(x_text)
            y = int(y_text)
        except ValueError:
            continue

        points.append((x, y, char))

    print("\nPoints Found:", len(points))

    if not points:
        return ""

    max_x = max(x for x, y, char in points)
    max_y = max(y for x, y, char in points)

    print("Max X:", max_x)
    print("Max Y:", max_y)
    print("Y Values:", sorted(set(y for x, y, char in points)))

    grid = [
        [" "] * (max_x + 1)
        for _ in range(max_y + 1)
    ]

    for x, y, char in points:
        grid[y][x] = char

    output_lines = []

    for y in range(max_y, -1, -1):
        output_lines.append("".join(grid[y]))

    output = "\n".join(output_lines)

    print("\nDecoded grid:\n")
    print(output)

    return output


decode_secret_message(
    "https://docs.google.com/document/d/e/2PACX-1vTMOmshQe8YvaRXi6gEPKKlsC6UpFJSMAk4mQjLm_u1gmHdVVTaeh7nBNFBRlui0sTZ-snGwZM4DBCT/pub"
)
