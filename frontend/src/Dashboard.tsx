import { useEffect, useState } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    LineElement,
    PointElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js'

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    LineElement,
    PointElement,
    Title,
    Tooltip,
    Legend
)

// Типы API ответов
interface ScoreBucket {
    bucket: string
    count: number
}

interface TimelinePoint {
    date: string
    submissions: number
}

interface PassRate {
    task: string
    passed: number
    total: number
}

const labs = ['lab-01', 'lab-02', 'lab-03', 'lab-04'] // пример

export default function Dashboard() {
    const [lab, setLab] = useState(labs[0])
    const [scores, setScores] = useState<ScoreBucket[]>([])
    const [timeline, setTimeline] = useState<TimelinePoint[]>([])
    const [passRates, setPassRates] = useState<PassRate[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    const token = localStorage.getItem('api_key') || ''

    useEffect(() => {
        if (!token) return
        setLoading(true)
        setError(null)

        const fetchData = async () => {
            try {
                const [scoresRes, timelineRes, passRes] = await Promise.all([
                    fetch(`/analytics/scores?lab=${lab}`, {
                        headers: { Authorization: `Bearer ${token}` },
                    }),
                    fetch(`/analytics/timeline?lab=${lab}`, {
                        headers: { Authorization: `Bearer ${token}` },
                    }),
                    fetch(`/analytics/pass-rates?lab=${lab}`, {
                        headers: { Authorization: `Bearer ${token}` },
                    }),
                ])

                if (!scoresRes.ok) throw new Error(`Scores ${scoresRes.status}`)
                if (!timelineRes.ok) throw new Error(`Timeline ${timelineRes.status}`)
                if (!passRes.ok) throw new Error(`PassRates ${passRes.status}`)

                setScores(await scoresRes.json())
                setTimeline(await timelineRes.json())
                setPassRates(await passRes.json())
            } catch (err: any) {
                setError(err.message || 'Fetch error')
            } finally {
                setLoading(false)
            }
        }

        fetchData()
    }, [lab, token])

    const barData = {
        labels: scores.map((s) => s.bucket),
        datasets: [
            {
                label: 'Score distribution',
                data: scores.map((s) => s.count),
                backgroundColor: 'rgba(75, 192, 192, 0.5)',
            },
        ],
    }

    const lineData = {
        labels: timeline.map((t) => t.date),
        datasets: [
            {
                label: 'Submissions per day',
                data: timeline.map((t) => t.submissions),
                borderColor: 'rgba(53, 162, 235, 0.7)',
                backgroundColor: 'rgba(53, 162, 235, 0.3)',
            },
        ],
    }

    if (loading) return <p>Loading dashboard...</p>
    if (error) return <p>Error: {error}</p>

    return (
        <div>
            <h1>Dashboard</h1>

            <label>
                Select lab:{' '}
                <select value={lab} onChange={(e) => setLab(e.target.value)}>
                    {labs.map((l) => (
                        <option key={l} value={l}>
                            {l}
                        </option>
                    ))}
                </select>
            </label>

            <div style={{ maxWidth: 600, margin: '20px 0' }}>
                <Bar data={barData} />
            </div>

            <div style={{ maxWidth: 600, margin: '20px 0' }}>
                <Line data={lineData} />
            </div>

            <h2>Pass Rates</h2>
            <table>
                <thead>
                    <tr>
                        <th>Task</th>
                        <th>Passed</th>
                        <th>Total</th>
                        <th>Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {passRates.map((p) => (
                        <tr key={p.task}>
                            <td>{p.task}</td>
                            <td>{p.passed}</td>
                            <td>{p.total}</td>
                            <td>{((p.passed / p.total) * 100).toFixed(1)}%</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    )
}