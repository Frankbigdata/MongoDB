db.Felicidad.find()

// Obtenemos el numero de documentos para Felicidad

var numero_documentos = db.Felicidad.countDocuments();

print("Numero de documentos en la coleccion Felicidad: " + numero_documentos);

// PROYECTAMOS solo los campos "Country or region" y "Score"

db.Felicidad.find({}, { "Country or region": 1, "Score": 1, "_id": 0 });

// Proyectamos países con un puntaje mayor a 7, mostrando solo "Country or region" y "Score"

db.Felicidad.find({ "Score": { $gt: 7 } }, { "Country or region": 1, "Score": 1, "_id": 0 });



// INSERTAR (Insertamos algunos datos ficticios)

var inserts = [
  { "Country": "Inventado", "Score": 7.5 },
  { "Country": "Inventado2", "Score": 6.8, "GDP": 1.2, "SocialSupport": 1.4 }
];
db.Felicidad.insertMany(inserts);



// ACTUALIZAR (Actualizamos el indice de felicidad y el GDP de los paises inventados)

db.Felicidad.update({ "Country": "Inventado" }, { $set: { "Score": 6.0 } })
db.Felicidad.update({ "Country": "Inventado2" }, { $set: { "Score": 5, "GDP": 0.2 } 


// ESTADISTICAS GLOBALES (calculamos la media, el minimo y el maximo de Score y GDP)

var estadisticas_globales = [
  {
    $group: {
      _id: null,
      avgScore: { $avg: "$Score" },
      minScore: { $min: "$Score" },
      maxScore: { $max: "$Score" },
      avgGDP: { $avg: "$GDP" },
      minGDP: { $min: "$GDP" },
      maxGDP: { $max: "$GDP" },
    }
  }
];

db.Felicidad.aggregate(estadisticas_globales);


// País más feliz
db.Felicidad.find().sort({ "Score": -1 }).limit(1)

// País menos feliz
db.Felicidad.find().sort({ "Score": 1 }).limit(1)

// MOSTRAMOS LOS 5 PAISES CON MAYOR INDICE PER CAPITA GDP

var top5GDPPerCapita = [{ $sort: { "GDP per capita": -1 } }, { $limit: 5 }, { $project: { _id: 0, "Country or region": 1, "GDP per capita": 1 } }];
db.Felicidad.aggregate(top5GDPPerCapita);


// COMPARAMOS QATAR QUE ES EL QUE MAS GDP TIENE CON OTROS PAISES UTILIZANDO LA FUNCION FACET

var comparar_qatar = [
  { $sort: { "GDP per capita": -1 } },
  {
    $facet: {
      highestGDP: [{ $limit: 1 }, { $project: { _id: 0, country: "$Country or region", GDP: "$GDP per capita" } }],
      otherCountries: [{ $skip: 1 }, { $project: { _id: 0, country: "$Country or region", GDP: "$GDP per capita" } }],
    },
  },
];

db.Felicidad.aggregate(comparar_qatar);



// DISTRIBUIMOS GENEROSIDAD ORDENADA DE PAIS MAS GENEROSO A MENOS GENEROSO, CON SU MEDIA, MINIMO Y MAXIMO

var distribucion_generosidad = [
  {
    $group: {
      _id: "$Country or region",
      averageGenerosity: { $avg: "$Generosity" },
      minGenerosity: { $min: "$Generosity" },
      maxGenerosity: { $max: "$Generosity" },
      count: { $sum: 1 },
    },
  },
  {
    $sort: { averageGenerosity: -1 },
  },
];

db.Felicidad.aggregate(distribucion_generosidad);

// ORDENAMOS LOS 5 PAISES CON MEJOR APOYO SOCIAL


var Mejor_apoyo_social = [{ $sort: { "Social support": -1 } }, { $limit: 5 }];

db.Felicidad.aggregate(Mejor_apoyo_social);


// A continuacion, hemos removido dos decimales de nuestro dataset

var remover_decimales = [
  {
    $set: {
      "Score": { $round: ["$Score", 2] },
      "GDP per capita": { $round: ["$GDP per capita", 2] },
      "Healthy life expectancy": { $round: ["$Healthy life expectancy", 2] },
      "Freedom to make life choices": { $round: ["$Freedom to make life choices", 2] },
      "Generosity": { $round: ["$Generosity", 2] },
      "Perceptions of corruption": { $round: ["$Perceptions of corruption", 2] },
    },
  },
];

db.Felicidad.aggregate(remover_decimales);

//Contamos la cantidad de países en cada rango de puntaje de felicidad:

var rango_felicidad = [
  {
    $bucket: {
      groupBy: "$Score",
      boundaries: [0, 3, 5, 7, 10],
      default: "Other",
      output: {
        count: { $sum: 1 }
      }
    }
  }
];

db.Felicidad.aggregate(rango_felicidad);


// MOSTRAMOS LOS 5 PAISES DONDE LA PERCEPCION DE CORRUPCION ES MAS BAJA

var percepcion_corrupcion = [
  { $sort: { "Perceptions of corruption": -1 } },
  { $limit: 5 },
  { $project: { _id: 0, "Country or region": 1, "Perceptions of corruption": 1 } },
];
db.Felicidad.aggregate(percepcion_corrupcion);



// OBTENEMOS EL MAXIMO Y EL MINIMO TOTAL 

var Masfelices_minymax = [
  { $group: { _id: null, maxScore: { $max: "$Score" }, minScore: { $min: "$Score" } } }
];

var result = db.Felicidad.aggregate(Masfelices_minymax);

printjson(result.toArray());



// Calculamos el total GDP de todos los paises

var sumStage = { $group: { _id: null, totalGDP: { $sum: "$GDP per capita" } } };
db.Felicidad.aggregate([sumStage]);


// Obtenemos el pais menos corrupto

db.Felicidad.find().sort({ "Perceptions of corruption": -1 }).limit(1)


// Añadimos un campo de fecha ficticio a dos documentos

db.Felicidad.updateOne(
  { "Country or region": "Finland" },
  { $set: { "fechaCreacion": ISODate("2022-01-10T12:00:00Z") } }
);

db.Felicidad.updateOne(
  { "Country or region": "Denmark" },
  { $set: { "fechaCreacion": ISODate("2022-01-12T15:30:00Z") } }
);

// Encontramos documentos creados después de la fecha específica

var fechaLimite = ISODate("2022-01-11T00:00:00Z");
db.Felicidad.find({ "fechaCreacion": { $gt: fechaLimite } });

// Encontramos documentos creados entre dos fechas

var fechaInicio = ISODate("2022-01-09T00:00:00Z");
var fechaFin = ISODate("2022-01-14T23:59:59Z");
db.Felicidad.find({ "fechaCreacion": { $gte: fechaInicio, $lte: fechaFin } });


// Restamos 3 días a la fecha actual

var diasParaRestar = 3;
var aggPipeline = [{ $addFields: { "fechaCreacion": { $subtract: [{ $toDate: "$fechaCreacion" }, { $multiply: [diasParaRestar * 24 * 60 * 60 * 1000, -1] }] } } }];
db.Felicidad.aggregate(aggPipeline);



