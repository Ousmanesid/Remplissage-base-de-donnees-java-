import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class App {
    public static void main(String[] args) {
        Connection connexionBd = null;
        PreparedStatement declarationPrep = null;

        try {
            // Établir une connexion à la base de données SQLite
            Class.forName("org.sqlite.JDBC");
            connexionBd = DriverManager.getConnection("jdbc:sqlite:C:\\Users\\but-info\\Downloads\\db_hydro.db");

            // Paramètres de la requête
            String apiUrl = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/referentiel/stations";

            // Établir une connexion à l'API
            URL url = new URL(apiUrl);
            HttpURLConnection connexion = (HttpURLConnection) url.openConnection();
            connexion.setRequestMethod("GET");
            int statut = connexion.getResponseCode();

            if (statut == 206) {
                // Lire la réponse de l'API
                BufferedReader lecteur = new BufferedReader(new InputStreamReader(connexion.getInputStream()));
                String ligne;
                StringBuilder contenu = new StringBuilder();
                while ((ligne = lecteur.readLine()) != null) {
                    contenu.append(ligne);
                }
                lecteur.close();

                // Parser les données JSON
                JSONObject reponseJson = new JSONObject(contenu.toString());
                JSONArray donnees = reponseJson.getJSONArray("data");

                // Préparer la requête d'insertion
                String requeteInsertion = "INSERT OR IGNORE INTO stations (code_station, code_site, libelle_site, libelle_station, type_station, coordonnee_x_station, coordonnee_y_station, longitude_station, latitude_station, altitude_ref_alti_station, code_commune_station, libelle_commune, code_departement, code_region, libelle_region, date_maj_station, date_ouverture_station, en_service) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                declarationPrep = connexionBd.prepareStatement(requeteInsertion);

                // Insérer les données dans la base de données
                for (int i = 0; i < donnees.length(); i++) {
                    JSONObject station = donnees.getJSONObject(i);

                    // Récupérer les valeurs des champs
                    String codeStation = station.getString("code_station");
                    String codeSite = station.getString("code_site");
                    String libelleSite = station.getString("libelle_site");
                    String libelleStation = station.getString("libelle_station");
                    String typeStation = station.optString("type_station", null);
                    int coordonneeXStation = station.getInt("coordonnee_x_station");
                    int coordonneeYStation = station.getInt("coordonnee_y_station");
                    float longitudeStation = (float) station.getDouble("longitude_station");
                    float latitudeStation = (float) station.getDouble("latitude_station");
                    float altitudeRefAltiStation = (float) station.optDouble("altitude_ref_alti_station", 0);
                    String codeCommuneStation = station.getString("code_commune_station");
                    String libelleCommune = station.optString("libelle_commune","");
                    String codeDepartement = station.optString("code_departement", null);
                    String codeRegion = station.optString("code_region", null);
                    String libelleRegion = station.optString("libelle_region","");
                    String dateMajStationStr = station.optString("date_maj_station","");
                    String dateOuvertureStationStr = station.optString("date_ouverture_station","");
                    boolean enService = station.getBoolean("en_service");

                    // Convertir les dates
                    Timestamp timestampMaj = null;
                    Timestamp timestampOuverture = null;
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    
                    if (!dateMajStationStr.isEmpty()) {
                        Date parsedDateMaj = dateFormat.parse(dateMajStationStr);
                        timestampMaj = new Timestamp(parsedDateMaj.getTime());
                    }
                    
                    if (!dateOuvertureStationStr.isEmpty()) {
                        Date parsedDateOuverture = dateFormat.parse(dateOuvertureStationStr);
                        timestampOuverture = new Timestamp(parsedDateOuverture.getTime());
                    }
                    

                    // Définir les valeurs pour la requête d'insertion
                    declarationPrep.setString(1, codeStation);
                    declarationPrep.setString(2, codeSite);
                    declarationPrep.setString(3, libelleSite);
                    declarationPrep.setString(4, libelleStation);
                    if (typeStation != null) {
                        declarationPrep.setString(5, typeStation);
                    } else {
                        declarationPrep.setNull(5, java.sql.Types.VARCHAR); // Si la valeur est null : insérez une valeur NULL dans la base de données
                    }
                    declarationPrep.setInt(6, coordonneeXStation);
                    declarationPrep.setInt(7, coordonneeYStation);
                    declarationPrep.setFloat(8, longitudeStation);
                    declarationPrep.setFloat(9, latitudeStation);
                    if (!station.isNull("altitude_ref_alti_station")) {
                        declarationPrep.setFloat(10, altitudeRefAltiStation);
                    } else {
                        declarationPrep.setNull(10, java.sql.Types.FLOAT); // Si la valeur est nul...
                    }
                    declarationPrep.setString(11, codeCommuneStation);
                    declarationPrep.setString(12, libelleCommune);
                    if (codeDepartement != null) {
                        declarationPrep.setString(13, codeDepartement);
                    } else {
                        declarationPrep.setNull(13, java.sql.Types.VARCHAR); // Si la valeur est null
                    }
                    if (codeRegion != null) {
                        declarationPrep.setString(14, codeRegion);
                    } else {
                        declarationPrep.setNull(14, java.sql.Types.VARCHAR); // Si la valeur est null
                    }
                    declarationPrep.setString(15, libelleRegion);
                    if (timestampMaj != null) {
                        declarationPrep.setTimestamp(16, timestampMaj);
                    } else {
                        declarationPrep.setNull(16, java.sql.Types.TIMESTAMP); // Si la date est null
                    }
                    
                    if (timestampOuverture != null) {
                        declarationPrep.setTimestamp(17, timestampOuverture);
                    } else {
                        declarationPrep.setNull(17, java.sql.Types.TIMESTAMP); // Si la date est null
                    }
                    declarationPrep.setBoolean(18, enService);

                    // Exécuter la requête d'insertion
                    declarationPrep.executeUpdate();
                }
            } else {
                System.out.println("Erreur HTTP: " + statut);
            }
            connexion.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Fermer la connexion et la déclaration préparée
            try {
                if (declarationPrep != null) {
                    declarationPrep.close();
                }
                if (connexionBd != null) {
                    connexionBd.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
