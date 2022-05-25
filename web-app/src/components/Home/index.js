import "./home.css";
import { Footer, Navigation } from "nde-design-system";
import SwaggerUI from "swagger-ui-react";
import "swagger-ui-react/swagger-ui.css";
import swaggerJson from './swaggerJson.json';

function Home() {

    return (
        <div className="home-container">
            <div className="nav-container">
                <Navigation />
            </div>
            <div className="body-test">
                <SwaggerUI spec={swaggerJson} />
            </div>
            <div className="footer-container">
                <Footer />
            </div>
        </div>
    );

};

export default Home;
