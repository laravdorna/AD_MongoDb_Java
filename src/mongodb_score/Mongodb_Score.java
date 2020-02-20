/*

ABRIR SERVIDOR MONGO: terminal mongod
 * EJercicio de prueba de conexión y utilizacion de mongobd
 DB: TENDA - DOCUMENTO- PEDIDOS

 1) inserir este  documento : "p4",c1","pro3",5,"02/02/2019"
 2) actualizar un par clave valor por clave do documento : exemplo actualizar o campo codpro do pedido  p3 a o valor pro4
 3)incrementar un par clave valor por clave. exemplo: aumentar en 7 a cantidade correspondente ao pedido p2 .
 4) amosar o documento correspondente ao pedido p3
 5) amosar o codigo do cliente, o codigo do producto e a cantidade correspondentes ao pedido p1
 6) amosar  o codigo do cliente e  o codigo do producto correspondentes aos pedidos que teñan no campo cantidade un valor superior a 2
 7) amosar  o codigo do cliente e  o codigo do producto correspondentes aos pedidos que teñan no campo cantidade un valor superior a 2 e inferior a 5
 8) amosar   o codigo do cliente e  o codigo do producto correspondentes a todos os pedidos.
 9) aumentar  no seu dobre a cantidade correspondente ao pedido p4.
            
 */

/*
 AÑADIR LIBRERIA 
 /home/oracle/drivers/mogno-java-driver-3.4.2.jar
 */
package mongodb_score;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import com.mongodb.client.model.Projections;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.include;
import org.bson.Document;
import static com.mongodb.client.model.Updates.inc;

;

/**
 *
 * @author Lara
 */
public class Mongodb_Score {

    //https://mongodb.github.io/mongo-java-driver/3.4/driver/getting-started/quick-start/
    //inicializamos elementos que se van a utilizar para la conexion
    public static MongoClient mongoClient = null;
    public static MongoDatabase database = null;
    public static MongoCollection<Document> collection = null;

    public static void main(String[] args) {
        //abre todas las conexiones
        conexion();
//1.
        insertar("p4", "c1", "pro3", 5, "02/02/2019");
//2
        actualizarPar("p3", "pro4");
       
//3
        incrementarPar("p2", 7);
//4
        mostrarDoc("p3");
//5
        mostrarDocFiltrado("p1");
//6
        mostrarDCantidadeMayorA("cantidade", 2);
//7
        mostrarDocEntreCantidades("cantidade", 2, 5);
//8
        mostrarTodosDoc();
//9
        multiplicarCantUnPedido("p4", 2);

        //cerrar cliente 
        mongoClient.close();
        /*
         metodos que muestran la db y las colecciones
         System.out.println("Bases de datos:"
         + database.getName());
         System.out.println("Colecciones: ");
         */
    }

    public static void conexion() {
        String ip = "localhost";
        int puerto = 27017;
        String db = "tenda";
        String cll = "pedidos";
        mongoClient = new MongoClient(ip, puerto);
        database = mongoClient.getDatabase(db);
        collection = database.getCollection(cll);

    }

    // 1) inserir este  documento : "p4",c1","pro3",5,"02/02/
    public static void insertar(String id, String ccli, String cpro, double cant, String fecha) {
        Document document = new Document();

        document.put("_id", id);
        document.put("codcli", ccli);
        document.put("codpro", cpro);
        document.put("cantidade", cant);
        document.put("data", fecha);

        collection.insertOne(document);

    }

    //2) actualizar un par clave valor por clave do documento : exemplo actualizar o
    //campo codpro do pedido  p3 a o valor pro4
    public static void actualizarPar(String clave, String codpro) {
        //*****EN UNA SOLA LINEA*****
        collection.updateOne(eq("_id", clave), new Document("$set", new Document("codpro", codpro)));

        //EN VARIAS LINEAS
//        Document query = new Document();
//        query.put("_id", clave);
//
//        Document newDocument = new Document();
//        newDocument.put("codpro", codpro);
//
//        Document updateObject = new Document();
//        updateObject.put("$set", newDocument);
//
//        collection.updateOne(query, updateObject);
    }

    // 3)incrementar un par clave valor por clave. exemplo: aumentar en 7 a cantidade correspondente ao pedido p2 .
    public static void incrementarPar(String clave, double incremento) {
        collection.updateOne(eq("_id", clave), inc("cantidade", incremento));

        //EN VARIAS LINEAS
//        Document query = new Document();
//        query.put("_id", clave);
//        
//        Document newDocument = new Document();
//        newDocument.put("cantidade", incremento);
//        
//        Document updateObject = new Document();
//        updateObject.put("$inc", newDocument);
//        
//        collection.updateOne(query, updateObject);
    }

    //4) amosar o documento correspondente ao pedido p3
    public static void mostrarDoc(String id) {
        Document documento = collection.find(eq("_id", id)).projection(include("codcli",
                "codpro", "cantidade")).first();

        String codcli = documento.getString("codcli");
        String codpro = documento.getString("codpro");
        double cantidade = documento.getDouble("cantidade");
        System.out.println("_id: " + id + ", codcli: " + codcli + ", codpro: "
                + codpro + ", cantidade: " + cantidade);

    }

    // 5) amosar o codigo do cliente, o codigo do producto e a cantidade correspondentes ao pedido p1
    public static void mostrarDocFiltrado(String id) {

        Document documento = collection.find(eq("_id", id)).projection(include("codcli",
                "codpro", "cantidade")).first();

        String codcli = documento.getString("codcli");
        String codpro = documento.getString("codpro");
        double cantidade = documento.getDouble("cantidade");
        System.out.println("_id: " + id + ", codcli: " + codcli + ", codpro: "
                + codpro + ", cantidade: " + cantidade);
    }

    //  6) amosar  o codigo do cliente e  o codigo do producto correspondentes aos 
    //pedidos que teñan no campo cantidade un valor superior a 2
    public static void mostrarDCantidadeMayorA(String clave, double valor) {
        FindIterable<Document> cursornovo = collection.find(gt(clave, valor)).projection(include("codcli",
                "codpro"));

        for (Document doc : cursornovo) {
            String codcli = doc.getString("codcli");
            String codpro = doc.getString("codpro");

            System.out.println("codcli: " + codcli + ", codpro: " + codpro);
        }
    }

    //7) amosar  o codigo do cliente e  o codigo do producto correspondentes aos
    //pedidos que teñan no campo cantidade un valor superior a 2 e inferior a 5
    public static void mostrarDocEntreCantidades(String clave, double mayor, double menor) {
        FindIterable<Document> cursornovo = collection.find(and(gt(clave, mayor),
                lt(clave, menor))).projection(include("codcli", "codpro"));

        for (Document doc : cursornovo) {
            String codcli = doc.getString("codcli");
            String codpro = doc.getString("codpro");
            System.out.println("codcli: " + codcli + ", codpro: " + codpro);
        }
    }

    // 8) amosar  o codigo do cliente e  o codigo do producto correspondentes a todos os pedidos.
    
     public static void mostrarTodosDoc() {
        FindIterable<Document> cursornovo = collection.find().projection(include("codcli",
                "codpro")).projection(exclude("_id"));

        //al  imprimir ese campo excluido  devuelve 'null'
        for (Document doc : cursornovo) {
            String codcli = doc.getString("codcli");
            String codpro = doc.getString("codpro");

            System.out.println("codcli: " + codcli + ", codpro: " + codpro);
        }
    }
    
   // 9) aumentar  no seu dobre a cantidade correspondente ao pedido p4.
    public static void multiplicarCantUnPedido(String id,int num) {
        Document first = collection.find(eq("_id", id)).first();

        double cantidade = first.getDouble("cantidade");

        cantidade = cantidade * num;

        collection.updateOne(eq("_id", id), new Document("$set", new Document("cantidade", cantidade)));
    }
     
     
}
