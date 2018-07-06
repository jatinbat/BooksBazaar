const express = require('express')
const bodyParser = require('body-parser');
const crypto = require('crypto') ;
const app = express()
const mysql = require('mysql2');
var jwt = require('jsonwebtoken');
var multer = require('multer');
var path = require('path')

var storage = multer.diskStorage({
    destination: function (req, file, cb) {
      cb(null, './public/assets/images/listings/')
    },
    filename: function (req, file, cb) {
      cb(null, Date.now().toString() + '-' + file.originalname) 
    }
  })
   
var upload = multer({ storage: storage }) ;

// create the connection to database
const connection = mysql.createConnection({
    host: 'sql12.freemysqlhosting.net',
    user: 'sql12245265',
    password : 'upsLUJk7Bh' ,
    database : 'sql12245265' ,
    multipleStatements: true 
  });
  
app.use( express.static('public') ) ;  
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}) );

app.all("/*", function(req, res, next){
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Content-Length, X-Requested-With');
  next();
});

function generateJwt( user_id ) {
    var expiry = new Date();
    expiry.setDate(expiry.getDate() + 7);

    return jwt.sign({
        user_id: user_id ,
        exp: parseInt(expiry.getTime() / 1000),
    }, "MY_SECRET"); 
}

app.post("/api/register" , function(req,res) {

    this.salt = crypto.randomBytes(16).toString('hex');
    this.hash = crypto.pbkdf2Sync(req.body.password, this.salt, 1000, 64, 'sha512').toString('hex');
    let query = `insert into users ( user_id , email , salt , hash ) values ( ? , ? , ? , ?) ;`
    
    connection.query( 
        query , [ req.body.username , req.body.email , this.salt , this.hash] ,
        function( err , results , fields ) {
            if (err) {
                console.log( "Error " + err ) ;
            }
            console.log("Query Done") ;
        }
     ) ;
}) ;


app.post('/api/login', function (req, res) {

    connection.query( 
        `select * from users where user_id = ?`, [req.body.username] ,
        function( err , results , fields ) {
            if (results.length === 0) {
                res.json({
                    statusCode : 500 ,
                });
            }else {

               let hash = results[0].hash ;
               let salt = results[0].salt ;
               
               let newHash =  crypto.pbkdf2Sync(req.body.password, salt, 1000, 64, 'sha512').toString('hex');

               if (newHash === hash) {
                   res.json({
                        statusCode : 200 ,
                        jwt : generateJwt(req.body.username) 
                   });
               }else {
                    res.json({
                        statusCode : 501 ,
                    });
                }

            }
            
        }
     ) ;
})

app.get( "/api/fetchListings" , function(req,res) {
    connection.query( 
        `select * from listings` ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json(results) ;
        }
     ) ;
} ) ;

app.post('/api/verifyToken', function (req, res) {
    
    jwt.verify(req.body.token, 'MY_SECRET', (err, authData) => {
        if(err) {
            res.json({
                statusCode : 500 ,
            });
        } else {
          res.json({
              statusCode : 200 ,
                authData
          });
        }
      });

}) ;

app.post('/api/addListingToWishlist' , function(req,res) {
    
    connection.query( 
        `insert into wishlist ( user_id , listing_id ) values (?,?) `, [req.body.user_id , req.body.listing_id] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json("Done") ;
        }
     ) ;    

})

app.post('/api/fetchWishlist' , function(req,res){
    
    connection.query( 
        `select * from listings inner join wishlist on listings.listing_id=wishlist.listing_id where user_id = ? ; `, [req.body.user_id] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json( results ) ;
        }
     ) ;    

}) ;

app.post('/api/fetchMyListings', function(req,res) {
    connection.query( 
        `select * from listings where seller = ? ; `, [req.body.user_id] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json( results ) ;
        }
     ) ;    
}) ;



app.post('/api/uploadImage' , upload.single('avatar'), function(req,res) {
    connection.query( 
        `insert into listings ( seller , book_name , book_author , price , book_condition , image , other_details) values ( ? , ? , ? , ? ,? , ?, ? ) ;` ,
        [ req.body.seller , req.body.book_name , req.body.book_author , req.body.price , req.body.book_condition, req.file.path.split("\\")[4], req.body.other_details ],
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json(results) ;
        } ) ;
}) ;

app.post('/api/removefromMyListings' , function(req,res) {
    connection.query( 
        `delete from listings where listing_id = ? ; `, [req.body.listing_id] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json( "DOne" ) ;
        }
     ) ;    
})

app.post('/api/fetchListing', function(req,res) {
    connection.query( 
        `select * from listings where listing_id = ? ; `, [req.body.listing_id] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
                res.json("Error") ;
            }
            res.json( results[0] ) ;
        }
     ) ;    
}) ;

app.post('/api/removefromWishlist' , function(req,res){
    

    connection.query( 
        `delete from wishlist where ( listing_id = ? and user_id = ? ) ;`, [req.body.listing_id , req.body.user_id] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            res.json("Done") ;
        }
     ) ;    

}) ;

app.post('/api/verifyUser', function(req,res) {

    connection.query( 
        `select * from users where user_id = ?`, [req.body.username] ,
        function( err , results , fields ) {
            if (err) {
                console.log("Error " + err) ;
            }
            if (results.length == 0) {
                res.json({val : true});
            }else {
                res.json({val : false});
            }   
        }
     ) ;

}) ;

app.post('/api/verifyEmail', function(req,res) {

    connection.query( 
        `select * from users where email = ?`, [req.body.email] ,
        function( err , results , fields ) {
            if (results.length == 0) {
                res.json({val : true});
            }else {
                res.json({val : false});
            }   
        }
     ) ;

}) ;




app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
}) 