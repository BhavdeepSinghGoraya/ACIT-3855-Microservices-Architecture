import React, { useEffect, useState } from 'react';
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [buyAnomaly, setBuyAnomaly] = useState(null);
    const [sellAnomaly, setSellAnomaly] = useState(null);
    const [error, setError] = useState(null);

    // Fetch Stats
    const getStats = () => {
        fetch(`http://acit-3855-mysql-kafka.francecentral.cloudapp.azure.com:/processing/stats`)
            .then((res) => res.json())
            .then(
                (result) => {
                    console.log("Received Stats");
                    setStats(result);
                    setIsLoaded(true);
                },
                (error) => {
                    setError(error);
                    setIsLoaded(true);
                }
            );
    };

    // Fetch Anomalies
    const getAnomalies = () => {
        // Fetch 'Too Low' anomalies for buy events
        fetch(`http://acit-3855-mysql-kafka.francecentral.cloudapp.azure.com/anomaly_detector/anomalies?anomaly_type=TooLow`)
            .then((res) => res.json())
            .then((result) => {
                console.log("Received Low Anomalies");
                const buyAnomalies = result.filter((anomaly) => anomaly.event_type === 'buy');
                if (buyAnomalies.length > 0) {
                    const latestBuyAnomaly = buyAnomalies.reduce((latest, anomaly) =>
                        new Date(anomaly.timestamp) > new Date(latest.timestamp) ? anomaly : latest
                    );
                    setBuyAnomaly(latestBuyAnomaly);
                }
            })
            .catch((error) => setError(error));

        // Fetch 'Too High' anomalies for sell events
        fetch(`http://acit-3855-mysql-kafka.francecentral.cloudapp.azure.com/anomaly_detector/anomalies?anomaly_type=TooHigh`)
            .then((res) => res.json())
            .then((result) => {
                console.log("Received High Anomalies");
                const sellAnomalies = result.filter((anomaly) => anomaly.event_type === 'sell');
                if (sellAnomalies.length > 0) {
                    const latestSellAnomaly = sellAnomalies.reduce((latest, anomaly) =>
                        new Date(anomaly.timestamp) > new Date(latest.timestamp) ? anomaly : latest
                    );
                    setSellAnomaly(latestSellAnomaly);
                }
            })
            .catch((error) => setError(error));
    };

    useEffect(() => {
        const interval = setInterval(() => {
            getStats();
            getAnomalies();
        }, 5000); // Fetch stats and anomalies every 5 seconds
        return () => clearInterval(interval);
    }, []);

    if (error) {
        return <div className={"error"}>Error found when fetching from API</div>;
    } else if (!isLoaded) {
        return <div>Loading...</div>;
    } else {
        return (
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"}>
                    <tbody>
                        <tr>
                            <th>Book Buy</th>
                            <th>Book Sell</th>
                        </tr>
                        <tr>
                            <td># Buy: {stats['num_buy_events']}</td>
                            <td># Sell: {stats['num_sell_events']}</td>
                        </tr>
                        <tr>
                            <td colSpan="2">Max Buy Price: {stats['max_buy_price']}</td>
                        </tr>
                        <tr>
                            <td colSpan="2">Max Sell Price: {stats['max_sell_price']}</td>
                        </tr>
                    </tbody>
                </table>
                <h3>Last Updated: {stats['last_updated']}</h3>

                {/* Display Latest Anomalies */}
                <h2>Latest Anomalies</h2>

                <div>
                    <h3>Buy Event (Too Low)</h3>
                    {buyAnomaly ? (
                        <>
                            <p>UUID: {buyAnomaly.event_id}</p>
                            <p>{buyAnomaly.description}</p>
                            <p>Detected on {buyAnomaly.timestamp}</p>
                        </>
                    ) : (
                        <p>No recent low buy anomalies detected.</p>
                    )}
                </div>

                <div>
                    <h3>Sell Event (Too High)</h3>
                    {sellAnomaly ? (
                        <>
                            <p>UUID: {sellAnomaly.event_id}</p>
                            <p>{sellAnomaly.description}</p>
                            <p>Detected on {sellAnomaly.timestamp}</p>
                        </>
                    ) : (
                        <p>No recent high sell anomalies detected.</p>
                    )}
                </div>
            </div>
        );
    }
}
