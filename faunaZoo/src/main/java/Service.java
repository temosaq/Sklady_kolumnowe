import com.faunadb.client.FaunaClient;
import com.faunadb.client.types.Value;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import static com.faunadb.client.query.Language.*;
public class Service {

    private static final String DB_NAME="dbZoo";
    private static final String DB_COLLECTION="zoo";
    private static final String DB_INDEX="zoo_by_area";
    Scanner scanner = new Scanner(System.in);


    private FaunaClient client;
    private final FaunaClient adminClient;

    public Service() {

        this.adminClient= FaunaClient.builder()
                .withSecret("SECRET_KEY")
                .build();
        try {
            this.getClient();
        }
        catch (ExecutionException | InterruptedException e) {
        System.out.println(e.getMessage());
        }
    }

    public void createDatabase() throws ExecutionException, InterruptedException {

        FaunaClient adminClient =
                FaunaClient.builder()
                        .withSecret("fnADukOMNZACAMf9UV5DATWhn39z-FmjxZaXvgZ9")
                        .build();
        adminClient.query(
            If(
                    Exists(Database(DB_NAME)),
                    Value(true),
                CreateDatabase(
                        Obj("name", Value(DB_NAME))
                )
            )
        ).get();

        adminClient.close();
    }
    private void getClient() throws ExecutionException, InterruptedException {
        Value value=this.adminClient.query(
                CreateKey(Obj("database", Database(Value(DB_NAME)), "role", Value("server")))
        ).get();
        String key=value.at("secret").to(String.class).get();
        this.client=this.adminClient.newSessionClient(key);
    }

    public void createCollection() throws ExecutionException, InterruptedException {
        System.out.println(

                client.query(
                            If( Exists(Collection(DB_COLLECTION)),
                                Value(true),
                                CreateCollection(Obj("name", Value("zoo")))
                ))
                        .get());
    }

    public void createIndex() throws ExecutionException, InterruptedException {
        System.out.println(
                client.query(
                        If(
                                Exists(Index(DB_INDEX)),
                                Value(true),
                        CreateIndex(
                                Obj(
                                        "name", Value(DB_INDEX),
                                        "source", Collection(Value(DB_COLLECTION)),
                                        "terms", Arr(Obj("field", Arr(Value("data"), Value("area"))))
                                )
                                )

                        )).get());
    }


    public void addDocuments() throws ExecutionException, InterruptedException {
        System.out.println(
                client.query(
                        Create(
                                Collection(Value(DB_COLLECTION)),
                                Obj(
                                        "data", Obj(
                                                "name", Value("Zoo Łódź"),
                                                "address", Value("{street: 'Jaracza', housenumber: '9'}"),
                                                "animals", Value("{'lew', 'tygrys', 'zebra', 'żyrafa'}"),
                                                "area", Value(11),
                                                "tourists", Value(1000)
                                        )
                                )
                        )
                ).get());

        System.out.println(
                client.query(
                        Create(
                                Collection(Value(DB_COLLECTION)),
                                Obj(
                                        "data", Obj(
                                                "name", Value("Zoo Wrocław"),
                                                "address", Value("{street: 'Prosta', housenumber: '50'}"),
                                                "animals", Value("{'gepard', 'ryś', 'puma', 'żbik'}"),
                                                "area", Value(17),
                                                "tourists", Value(1300)
                                        )
                                )
                        )
                ).get());
    }

    public void getZoo() throws ExecutionException, InterruptedException {
        System.out.println("Podaj id zoo do wyświetlenia: ");
        String id=scanner.nextLine();
        System.out.println(


                client.query(Get(Ref(Collection(DB_COLLECTION),Value(id))))
                        .get()
        );
    }

    public void getZooByArea() throws ExecutionException, InterruptedException {
        System.out.println("Podaj powierzchnię zoo, które chcesz chcesz wyświetlić: ");
        int area=scanner.nextInt();
        System.out.println(
                client.query(
                        Get(
                                Match(
                                        Index(Value(DB_INDEX)),
                                        Value(area)))).get());
    }

    public void updateZoo() throws ExecutionException, InterruptedException {

        System.out.println("Podaj ID zoo do aktualizacji: ");
        String id=scanner.nextLine(); //\n
        System.out.println("Podaj nową powierzchnię");
        String newArea=scanner.nextLine();
        System.out.println(
                client.query(
                        Update(
                                Ref(Collection(DB_COLLECTION),Value(id)),

                                Obj("data", Obj("area", Arr(Value("area"), Value(newArea))))))
                        .get());
    }

    public void replace() throws ExecutionException, InterruptedException {
        System.out.println("Podaj id zoo do zmiany ");
        String id=scanner.nextLine();
        System.out.println(
                client.query(
                        Replace(
                                Ref(Collection(DB_COLLECTION),Value(id)),
                                Obj("data", Obj("name", Value("Zoo Poznań")))))
                        .get());
    }

    public void deleteZoo() throws ExecutionException, InterruptedException {
        System.out.println("Podaj id zoo do usunięcia:");
        String id=scanner.nextLine();
        System.out.println(
                client.query(
                        Delete(Ref(Collection(DB_COLLECTION),Value(id))))
                        .get());
    }
}

