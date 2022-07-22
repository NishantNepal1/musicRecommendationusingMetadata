import playlistscrapper
import csv
import uri_extractor


def main():
    spotify_selfLoad = input('Do you want to load your spotify playlist? If yes enter Y ')
    name = ''
    x = ''
    uri = ''
    if spotify_selfLoad.upper() == 'Y':
        x = input("Do you want to import from your account(A) or from a URL(U)")
        if x.upper() == "A":
            x = playlistscrapper.main()
            name = "{display_name}".format(**x) + ".csv"
        else:
            name = uri_extractor.uri_extractor()


    else:
        name = input('Input File name (.csv) to load')
        x = 'A'

    file = open(name, "r", encoding="utf8")
    print(name)

    with file as f:
        reader = csv.reader(f)
        for row in reader:
            print(row)


if __name__ == '__main__':
    main()
