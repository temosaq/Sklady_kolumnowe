import java.util.Scanner;

public class Fauna {
    public static void main(String[] args) throws Exception {

        Service service = new Service();



        do {
            Scanner scanner = new Scanner(System.in);
            System.out.println("");
            System.out.println("<--------------ZOO-------------->");
            System.out.println("-- 1. Utwórz bazę");
            System.out.println("-- 2. Utwórz kolekcję");
            System.out.println("-- 3. Utwórz indeks");
            System.out.println("-- 4. Dodaj zoo");
            System.out.println("-- 5. Pobierz zoo po ID");
            System.out.println("-- 6. Pobierz zoo po powierzchni");
            System.out.println("-- 7. Aktualizuj zoo");
            System.out.println("-- 8. Przetwarzanie");
            System.out.println("-- 9. Usuń zoo");


            System.out.println("Podaj numer operacji do wykonania: ");
            String option = scanner.nextLine();
            switch (option) {
                case "1":
                    service.createDatabase();
                    break;

                case  "2":
                    service.createCollection();
               break;

                case "3":
                    service.createIndex();
                    break;

                case "4":
                    service.addDocuments();
                    break;

                case "5":
                    service.getZoo();
                    break;

                case "6":
                    service.getZooByArea();
                    break;

                case "7":
                    service.updateZoo();
                    break;

                case "8":
                    service.replace();
                    break;

                case "9":
                    service.deleteZoo();
                    break;

                default:
                    System.out.println("Błędna instrukcja");
            }
        } while (true);

    }

}
